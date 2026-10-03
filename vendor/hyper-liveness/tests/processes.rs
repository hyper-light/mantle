//! Node-pair liveness between real processes over real UDP sockets, each heartbeat proved by a
//! real flush of a real file: the usage mantle and focal make of it, through hyper-tokio's plane
//! socket and its kernel receive stamps.
//!
//! The supervisor (`a_stalled_disk_and_a_killed_node_are_suspected_and_no_live_one_is`) starts
//! `NODES` copies of this test binary as member processes (`member_process`, selected by
//! `HYPER_LIVENESS_NODE`). Each runs the crate as an owner does: it polls at the crate's wake, feeds
//! it what the plane socket received with the kernel's stamps and what its disk made durable,
//! queues the heartbeats it returns on the sealed plane, makes the liveness writes it asks for on
//! its own device thread (one thread, writing and flushing one file: `fdatasync` on Linux,
//! `F_FULLFSYNC` on macOS, `FlushFileBuffers` on Windows, through std), and charges each detector
//! the election the library's law gives over the round trips its streams measured. It reports what
//! the crate reports, its disk's state and its writes: its run's record and its flushes, the longest
//! and all told. The test times nothing of its own and derives no bound. The supervisor waits on
//! facts, each for as long as the members move toward it: a quiet period derived from what they
//! state (the longest of their judged pairs' `η + α` and their unjudged pairs' intervals, past
//! their longest write, their wakes' lateness and their reporting period) that passes with nothing
//! moving fails the wait with every member's last state. Quiet is only time in which the supervisor
//! heard every member, by the E2E harnesses' rule (`hyper_raft_e2e::quiet`) in this test's terms:
//! a member whose line is past its due by a retransmission timeout is unheard, and a check decides
//! nothing while one is; the time the members' writes took extends the wait; and a member silent
//! past its due by more than the longest write any member stated and the quiet period fails the
//! wait, named, its silence counted only over time the supervisor listened for its lines
//! (`Supervisor::until`):
//! - every member's every pair configured; then it stalls one member's disk (its device thread
//!   stops completing flushes, as a disk that stops does) and waits for every other member to
//!   suspect it, each within the bound its detector stated, measured from the stalled member's
//!   last heartbeat's schedule on the host's monotonic clock, which every process reads alike;
//! - then it kills another member with SIGKILL (TerminateProcess on Windows) and waits for every
//!   survivor to suspect it, each within its stated bound the same way.
//!
//! A second supervisor (`a_node_killed_in_its_first_heartbeats_is_suspected_once_a_sibling_has_its_evidence`)
//! starts three members and kills one with SIGKILL as soon as it says it has heard every peer and
//! sent to each, before any of its links could have its own evidence: each survivor, left one live
//! link, suspects it no later than its first poll once the link's freshness point has passed and
//! its live link has configured (`docs/timing.md` §3, item 10).
//!
//! A third (`a_stopped_member_fails_the_wait_that_needs_it_by_name`) stops one of three members once
//! every pair is configured and holds the waits to their bound: the wait for a line only the
//! stopped member can state fails, naming it; let go, it is trusted again.
//!
//! Each member keeps a record of what it fed its stream and what the stream told it, and writes it
//! out with each state line, in the same write (`record`): every heartbeat it fed, with its kernel
//! stamp, what the stream made of it and the trust it held of the peer after; every poll that told
//! a change, moved a trust or came at or past a point a peer was held trusted to; every change told;
//! every heartbeat sent. Every count a state line reports of a peer (the suspicions, the heartbeats
//! taken, refused for their proof and sent, the slots skipped) must be its record's, and at each
//! test's end every suspicion in the records, as far as each member wrote them whole, is traced to
//! the detector's rule exactly (`record::trace`): its heartbeat, its freshness point as the stream
//! held it, the call that noticed it at or past the point, no heartbeat of the peer's taken after
//! stamped before the point, and every heartbeat or poll past a held point telling its suspicion.
//! One that does not trace fails the test, named. The live members' suspicions against the
//! allowance their configurations promised are the model's figures, reported
//! (`docs/benchmarks.md`), never asserted: no run's count is a test of a bound on an expectation.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::print_stdout,
    clippy::cognitive_complexity,
    missing_docs
)]

use std::collections::BTreeMap;
use std::future::poll_fn;
use std::io::{BufRead, BufReader, Write as _};
use std::net::{SocketAddr, UdpSocket};
use std::pin::pin;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::task::Poll;
use std::time::Duration;

use hyper_datagram::{AdmitAll, ExporterSecret, Plane, PlaneLimits, Role, SECRET_BYTES};
use hyper_liveness::{
    Change, Heartbeat, Last, Liveness, Output, PairReport, PeerId, Refusal, Settings, Suspicion,
    Write, is_liveness,
};
use hyper_timing::{Ballot, Exposure, Trust, WINDOW_LIMIT};
use hyper_tokio::{Clock, Io, PlaneSocket};

#[path = "support/record.rs"]
mod record;
use record::{Beat, Entry, Record, Traced};

/// Members: one whose disk stalls, one killed, and two that watch both.
const NODES: u64 = 4;
/// The member whose disk the supervisor stalls.
const STALLED: u64 = 3;
/// The member the supervisor kills.
const KILLED: u64 = 4;
/// The plane's datagram on the path: QUIC's minimum, which every path carries (RFC 9000 §14.1).
const DATAGRAM: usize = 1_200;

fn secret_between(a: u64, b: u64) -> ExporterSecret {
    // Stands for the QUIC exporter both ends of a connection compute: one secret per pair.
    let (low, high) = (a.min(b), a.max(b));
    let mut bytes = [0u8; SECRET_BYTES];
    bytes[..8].copy_from_slice(&low.to_le_bytes());
    bytes[8..16].copy_from_slice(&high.to_le_bytes());
    ExporterSecret::new(bytes)
}

/// What the device thread sends the owner's wake socket: a flush completed, or the disk stopped.
/// The supervisor's commands are longer: [`STALL`] and [`HOLD`].
const COMPLETED: u8 = 0;
const STOPPED: u8 = 1;
/// The supervisor's command that the member's disk stop.
const STALL: &[u8] = b"stall";
/// The supervisor's command that the member hold its thread, answering nothing, until a byte comes
/// on its standard input: a member stopped on a platform with no signal that stops a process
/// (Windows), as hyper-raft-e2e's members are held (`hyper_raft_e2e::parent`).
const HOLD: &[u8] = b"hold";

/// The member's disk: one thread writing and flushing one file, a request at a time.
enum Request {
    Flush,
    /// The disk stops: nothing it is asked completes again.
    Stall,
}

fn device(
    path: std::path::PathBuf,
    requests: Receiver<Request>,
    done: SyncSender<(u64, u64)>,
    wake: UdpSocket,
) {
    use std::io::{Seek, SeekFrom, Write};
    let clock = Clock::new().unwrap();
    // Read access too: Windows answers a query of the file's volume on a handle that may read.
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    // A liveness write is one block of the size the system reports for the file, so the device
    // takes it whole and the flush is of that block alone.
    let block = vec![0xa5u8; hyper_block::file::preferred_block(&file, &path).unwrap()];
    while let Ok(request) = requests.recv() {
        match request {
            Request::Flush => {
                let started = clock.now_ns();
                file.seek(SeekFrom::Start(0)).unwrap();
                file.write_all(&block).unwrap();
                file.sync_data().unwrap();
                if done.send((started, clock.now_ns())).is_err() {
                    return;
                }
                // A wake for the owner; lost only if the owner is gone.
                let _ = wake.send(&[COMPLETED]);
            }
            Request::Stall => {
                // A disk that stopped: the thread says so, then holds its last request for ever.
                let _ = wake.send(&[STOPPED]);
                let (_keep, never) = sync_channel::<()>(0);
                let _ = never.recv();
                return;
            }
        }
    }
}

/// What the crate asks of the owner, gathered during one call.
struct Asked<'a> {
    plane: &'a mut Plane,
    flush: bool,
    changes: Vec<Change>,
    /// The heartbeats it sent: to whom, their run and number, for the member's record.
    sent: Vec<(PeerId, u64, u64)>,
}

impl Output for Asked<'_> {
    fn heartbeat(&mut self, peer: PeerId, message: &[u8]) {
        if let Ok(beat) = Heartbeat::decode(message) {
            self.sent.push((peer, beat.run, beat.seq));
        }
        // A message the plane refuses is a lost heartbeat, which the detector measures.
        let _ = self.plane.queue(peer, message);
    }
    fn flush(&mut self) {
        self.flush = true;
    }
    fn change(&mut self, change: Change) {
        self.changes.push(change);
    }
}

/// One member process: runs until it is killed or its supervisor is gone.
#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "real processes on the host's clock, threads and environment (CLAUDE.md §1a, end to end)"
)]
fn member_process() {
    let Ok(me) = std::env::var("HYPER_LIVENESS_NODE") else {
        return;
    };
    let me: u64 = me.parse().unwrap();
    let nodes: u64 = std::env::var("HYPER_LIVENESS_NODES")
        .unwrap()
        .parse()
        .unwrap();
    let file = std::path::PathBuf::from(std::env::var("HYPER_LIVENESS_FILE").unwrap());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(member(me, nodes, file));
}

#[allow(
    clippy::disallowed_methods,
    reason = "real processes on the host's clock, threads and environment (CLAUDE.md §1a, end to end)"
)]
async fn member(me: u64, nodes: u64, file: std::path::PathBuf) {
    // Its own port, the system's choice: a port picked for it and released could be taken in
    // between, by another group's member as two supervisors start at once.
    let mut socket =
        PlaneSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)), Io { batch: 64 }).unwrap();
    let port = socket.local_addr().unwrap().port();
    let clock = *socket.clock();
    let mut plane = Plane::new(
        me,
        PlaneLimits {
            max_peers: nodes as usize,
            epochs_per_peer: 2,
            window_limit: 1_024,
        },
    )
    .unwrap();
    // Its first write: its run, kept durably before its stream sends anything under it, on the
    // device its liveness writes go to. The first measure of that device it has.
    let (run, run_write) = raise_run(&file, &clock);
    let mut liveness = Liveness::new(Settings {
        local: me,
        run,
        max_peers: nodes as usize,
        history: Exposure::new(),
    })
    .unwrap();
    let peers: Vec<u64> = (1..=nodes).filter(|peer| *peer != me).collect();
    for &peer in &peers {
        let role = if me < peer {
            Role::Initiator
        } else {
            Role::Acceptor
        };
        plane
            .install_epoch(peer, 1, &secret_between(me, peer), role)
            .unwrap();
        plane.set_path(peer, DATAGRAM).unwrap();
        // One group of all the members.
        liveness.attach(peer).unwrap();
    }
    // The disk, and the socket its completions and the supervisor's commands wake the owner on.
    let wake = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let waker = UdpSocket::bind("127.0.0.1:0").unwrap();
    waker.connect(wake.local_addr().unwrap()).unwrap();
    // Room for the most requests ever outstanding: the one flush the owner keeps in flight, and
    // the stall. With room for one, a stall that came while a flush waited for the device thread
    // was refused and lost, and the member's disk never stopped.
    let (requests, requested) = sync_channel::<Request>(2);
    let (done, completions) = sync_channel::<(u64, u64)>(1);
    std::thread::spawn(move || device(file, requested, done, waker));

    let mut stdout = std::io::stdout();
    if writeln!(
        stdout,
        "ready {me} {} {port}",
        wake.local_addr().unwrap().port()
    )
    .and_then(|()| stdout.flush())
    .is_err()
    {
        return;
    }
    // Told to start with every member's port.
    let mut start = String::new();
    if !matches!(std::io::stdin().read_line(&mut start), Ok(read) if read > 0) {
        return;
    }
    let ports: Vec<u16> = start
        .trim()
        .trim_start_matches("start ")
        .split(',')
        .map(|port| port.parse().unwrap())
        .collect();
    let address = |id: u64| SocketAddr::from(([127, 0, 0, 1], ports[(id - 1) as usize]));

    // When the flush in flight was asked for, on the host clock.
    let mut flight: Option<u64> = None;
    let mut disk = Disk::Running;
    let mut flush_most = run_write;
    // The time its writes took, all told: time its heartbeats waited on its device.
    let mut blocked = run_write;
    let mut reported_at = 0u64;
    let mut heard_all = false;
    let mut command = [0u8; 16];
    // What the member fed its stream and what the stream told it, written out with its state
    // lines and traced by the supervisor (`record`).
    let mut record = Record::default();
    record.began();
    // The first poll: it asks for the flush that proves the first heartbeats.
    let mut first = Asked {
        plane: &mut plane,
        flush: false,
        changes: Vec::new(),
        sent: Vec::new(),
    };
    let now = clock.now_ns();
    liveness.poll(now, &mut first);
    record.polled(now, &first.changes, trusts(&liveness, &peers));
    for (peer, run, seq) in first.sent.drain(..) {
        record.sent(peer, run, seq);
    }
    if first.flush && requests.try_send(Request::Flush).is_ok() {
        flight = Some(now);
    }
    // Its first state, before it waits: its first flush in flight, which proves its first
    // heartbeats and on which a slow disk holds everything after.
    let member = Member {
        me,
        disk,
        flight,
        flush_most,
        blocked,
        flushed: false,
    };
    if report(
        &member,
        &liveness,
        &peers,
        &[],
        &mut record,
        now,
        &mut reported_at,
        &mut stdout,
    )
    .is_err()
    {
        return;
    }
    loop {
        // Wait for a datagram, a completion or a command, or the crate's wake.
        let wake_ns = liveness.wake();
        let began = clock.now_ns();
        let deadline = wake_ns
            .map(|at| tokio::time::Instant::now() + Duration::from_nanos(at.saturating_sub(began)));
        let mut woke_with = None;
        // What arrived, stamped by the kernel: fed before anything is judged at `now`.
        let mut inbox: Vec<(u64, u64, Vec<u8>)> = Vec::new();
        {
            let mut receive = pin!(socket.receive(&mut plane, &AdmitAll, |arrival, opened| {
                if let Ok(opened) = opened {
                    for message in opened.messages().filter(|m| is_liveness(m)) {
                        inbox.push((opened.sender, arrival.at_ns, message.to_vec()));
                    }
                }
            }));
            let mut woken = pin!(wake.recv(&mut command));
            let mut timer = pin!(async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending::<()>().await,
                }
            });
            poll_fn(|context| {
                if let Poll::Ready(result) = woken.as_mut().poll(context) {
                    woke_with = result.ok();
                    return Poll::Ready(());
                }
                if receive.as_mut().poll(context).is_ready() {
                    return Poll::Ready(());
                }
                if timer.as_mut().poll(context).is_ready() {
                    return Poll::Ready(());
                }
                Poll::Pending
            })
            .await;
        }
        // A wait begun before the crate's wake and ended at or past it, by its deadline or by
        // what came after it, is what its `G` is made of (`Liveness::on_wait`): reported before
        // anything the wait brought is fed.
        let woke = clock.now_ns();
        if let Some(at) = wake_ns
            && began < at
            && woke >= at
        {
            liveness.on_wait(at, woke);
        }
        let said = woke_with.and_then(|length| command.get(..length));
        if said == Some(&[STOPPED][..]) {
            disk = Disk::Stopped;
        }
        if said == Some(STALL) {
            disk = if requests.try_send(Request::Stall).is_ok() {
                Disk::Asked
            } else {
                Disk::Refused
            };
            continue;
        }
        if said == Some(HOLD) {
            // Says it is held, then holds, answering nothing, until the supervisor writes a byte
            // on standard input (or closes it, gone): no line it states can be stamped later.
            if writeln!(stdout, "held {me} {}", clock.now_ns())
                .and_then(|()| stdout.flush())
                .is_err()
            {
                return;
            }
            let _ = std::io::Read::read(&mut std::io::stdin(), &mut [0u8; 1]);
            continue;
        }
        let mut asked = Asked {
            plane: &mut plane,
            flush: false,
            changes: Vec::new(),
            sent: Vec::new(),
        };
        // The time the stream is judged at, read before the socket is: every datagram stamped
        // before it was in the socket by then, so the reads after take it, and the stream is fed
        // every message stamped before the time it is polled at. Read after them, a stop between
        // the reads and the clock (this test stops members) left heartbeats stamped before the
        // poll unread, and the poll suspected a peer whose next heartbeat had come.
        let now = clock.now_ns();
        // Read until a datagram stamped at `now` or later comes, the socket being in order of
        // arrival, or the socket is empty: `receive_ready` takes a bounded number of batches.
        let mut latest = None;
        while latest.is_none_or(|at| at < now) {
            let mut read = 0usize;
            socket
                .receive_ready(asked.plane, &AdmitAll, |arrival, opened| {
                    read += 1;
                    latest = Some(arrival.at_ns);
                    if let Ok(opened) = opened {
                        for message in opened.messages().filter(|m| is_liveness(m)) {
                            inbox.push((opened.sender, arrival.at_ns, message.to_vec()));
                        }
                    }
                })
                .unwrap();
            if read == 0 {
                break;
            }
        }
        for (from, at, message) in &inbox {
            let told = asked.changes.len();
            let outcome = liveness.on_heartbeat(*from, message, *at, &mut asked);
            if let Some(beat) = Beat::of(message, *at) {
                record.fed(
                    *from,
                    beat,
                    outcome,
                    &asked.changes[told..],
                    liveness.trust(*from),
                );
            }
        }
        let mut flushed = false;
        while let Ok((started, durable)) = completions.try_recv() {
            flight = None;
            flushed = true;
            flush_most = flush_most.max(durable.saturating_sub(started));
            blocked = blocked.saturating_add(durable.saturating_sub(started));
            liveness.on_durable(Write::Liveness, started, durable);
        }
        let told = asked.changes.len();
        liveness.poll(now, &mut asked);
        record.polled(now, &asked.changes[told..], trusts(&liveness, &peers));
        for (peer, run, seq) in asked.sent.drain(..) {
            record.sent(peer, run, seq);
        }
        if asked.flush && flight.is_none() && requests.try_send(Request::Flush).is_ok() {
            flight = Some(now);
        }
        let changes = std::mem::take(&mut asked.changes);
        socket.flush(&mut plane, |peer| Some(address(peer)), |_, _| {});
        elect(&mut liveness, &peers);
        let member = Member {
            me,
            disk,
            flight,
            flush_most,
            blocked,
            flushed,
        };
        if report(
            &member,
            &liveness,
            &peers,
            &changes,
            &mut record,
            now,
            &mut reported_at,
            &mut stdout,
        )
        .is_err()
        {
            return;
        }
        // Once it has heard every peer and sent to each: its links' first heartbeats.
        if !heard_all
            && peers.iter().all(|peer| {
                liveness
                    .report(*peer)
                    .is_some_and(|r| r.taken > 0 && r.sent > 0)
            })
        {
            heard_all = true;
            if writeln!(stdout, "heard {me}")
                .and_then(|()| stdout.flush())
                .is_err()
            {
                return;
            }
        }
    }
}

/// The trust the stream holds of each of `peers`, for the member's record.
fn trusts(liveness: &Liveness, peers: &[u64]) -> Vec<(PeerId, Option<Trust>)> {
    peers
        .iter()
        .map(|peer| (*peer, liveness.trust(*peer)))
        .collect()
}

/// A trust as a record line states it: trusted to a point, suspected, judged by no margin, or no
/// pair.
fn trust_field(trust: Option<Trust>) -> String {
    match trust {
        Some(Trust::Trusted { until_ns }) => format!("T{until_ns}"),
        Some(Trust::Suspected) => "S".to_owned(),
        Some(Trust::Unconfigured) => "U".to_owned(),
        None => "-".to_owned(),
    }
}

fn parse_trust(field: &str) -> Option<Option<Trust>> {
    match field {
        "S" => Some(Some(Trust::Suspected)),
        "U" => Some(Some(Trust::Unconfigured)),
        "-" => Some(None),
        until => Some(Some(Trust::Trusted {
            until_ns: until.strip_prefix('T')?.parse().ok()?,
        })),
    }
}

/// Every refusal the crate states, for a record line's: each by its name.
const REFUSALS: [Refusal; 13] = [
    Refusal::Limits,
    Refusal::TooManyPeers,
    Refusal::TooManyGroups,
    Refusal::UnknownPeer,
    Refusal::FromSelf,
    Refusal::Truncated,
    Refusal::NotLiveness,
    Refusal::BadVersion,
    Refusal::Malformed,
    Refusal::Stale,
    Refusal::Unproven,
    Refusal::Unmeasured,
    Refusal::OutOfRange,
];

fn parse_refusal(field: &str) -> Option<Option<Refusal>> {
    if field == "-" {
        return Some(None);
    }
    REFUSALS
        .iter()
        .find(|refusal| format!("{refusal:?}") == field)
        .map(|refusal| Some(*refusal))
}

/// The record's entries as lines, `r me kind fields`: a suspicion's with the latest the member had
/// woken past a wake it asked when it noticed it.
fn record_lines(me: u64, entries: &[Entry], liveness: &Liveness, out: &mut String) {
    use std::fmt::Write as _;
    let nanos = |d: Duration| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
    for entry in entries {
        let _ = match *entry {
            Entry::Began => writeln!(out, "r {me} b"),
            Entry::Fed {
                peer,
                beat,
                refused,
                holds,
            } => writeln!(
                out,
                "r {me} f {peer} {} {} {} {} {} {} {}",
                beat.run,
                beat.seq,
                beat.stamp,
                beat.due,
                beat.sent,
                refused.map_or_else(|| "-".to_owned(), |refusal| format!("{refusal:?}")),
                trust_field(holds)
            ),
            Entry::Polled { now } => writeln!(out, "r {me} p {now}"),
            Entry::Told(Change::Suspected(suspicion)) => writeln!(
                out,
                "r {me} s {} {} {} {} {} {}",
                suspicion.peer,
                suspicion.at_ns,
                suspicion.noticed_ns,
                suspicion.last.map_or_else(
                    || "-".to_owned(),
                    |last| format!(
                        "{},{},{},{}",
                        last.seq, last.arrival_ns, last.due_ns, last.sent_ns
                    )
                ),
                suspicion
                    .detection
                    .map_or_else(|| "-".to_owned(), |bound| nanos(bound).to_string()),
                nanos(liveness.latest_wake(suspicion.noticed_ns)),
            ),
            Entry::Told(Change::Trusted { peer, at_ns }) => {
                writeln!(out, "r {me} t {peer} {at_ns}")
            }
            Entry::Told(Change::Restarted { peer, at_ns }) => {
                writeln!(out, "r {me} n {peer} {at_ns}")
            }
            Entry::Holds { peer, trust } => {
                writeln!(out, "r {me} h {peer} {}", trust_field(trust))
            }
            Entry::Sent { peer, run, seq } => writeln!(out, "r {me} o {peer} {run} {seq}"),
        };
    }
}

/// A record line: the member, the entry, and for a suspicion the latest the member had woken past
/// a wake it asked when it noticed it.
fn parse_record(fields: &[&str]) -> Option<(u64, Entry, Option<u64>)> {
    let member = fields.first()?.parse().ok()?;
    let number = |at: usize| -> Option<u64> { fields.get(at)?.parse().ok() };
    let (entry, late) = match *fields.get(1)? {
        "b" => (Entry::Began, None),
        "f" => (
            Entry::Fed {
                peer: number(2)?,
                beat: Beat {
                    run: number(3)?,
                    seq: number(4)?,
                    stamp: number(5)?,
                    due: number(6)?,
                    sent: number(7)?,
                },
                refused: parse_refusal(fields.get(8)?)?,
                holds: parse_trust(fields.get(9)?)?,
            },
            None,
        ),
        "p" => (Entry::Polled { now: number(2)? }, None),
        "s" => {
            let last = match *fields.get(5)? {
                "-" => None,
                last => {
                    let parts: Vec<u64> = last
                        .split(',')
                        .map(|part| part.parse().ok())
                        .collect::<Option<_>>()?;
                    let [seq, arrival_ns, due_ns, sent_ns] = parts[..] else {
                        return None;
                    };
                    Some(Last {
                        seq,
                        arrival_ns,
                        due_ns,
                        sent_ns,
                    })
                }
            };
            let detection = match *fields.get(6)? {
                "-" => None,
                bound => Some(Duration::from_nanos(bound.parse().ok()?)),
            };
            (
                Entry::Told(Change::Suspected(Suspicion {
                    peer: number(2)?,
                    at_ns: number(3)?,
                    noticed_ns: number(4)?,
                    last,
                    detection,
                    detector: None,
                })),
                Some(number(7)?),
            )
        }
        "t" => (
            Entry::Told(Change::Trusted {
                peer: number(2)?,
                at_ns: number(3)?,
            }),
            None,
        ),
        "n" => (
            Entry::Told(Change::Restarted {
                peer: number(2)?,
                at_ns: number(3)?,
            }),
            None,
        ),
        "h" => (
            Entry::Holds {
                peer: number(2)?,
                trust: parse_trust(fields.get(3)?)?,
            },
            None,
        ),
        "o" => (
            Entry::Sent {
                peer: number(2)?,
                run: number(3)?,
                seq: number(4)?,
            },
            None,
        ),
        _ => return None,
    };
    Some((member, entry, late))
}

/// The member's run: the count kept beside its file raised by one (one where there is none), a
/// record kept whole and durable before the stream sends anything under it, as an owner keeps it
/// (`hyper_liveness::Settings::run`; hyper-raft-e2e's `run`), and how long the record's write took
/// on `clock`, nanoseconds: written whole, flushed with the platform's full flush and its directory
/// flushed after, on the device its liveness writes go to. A member started once on a fresh
/// directory is in its first.
fn raise_run(file: &std::path::Path, clock: &Clock) -> (u64, u64) {
    let mut name = file.as_os_str().to_owned();
    name.push(".run");
    let path = std::path::PathBuf::from(name);
    let previous = hyper_block::record::read(&path, 8)
        .unwrap()
        .map_or(0, |count| u64::from_le_bytes(count.try_into().unwrap()));
    let run = previous + 1;
    let started = clock.now_ns();
    hyper_block::record::write(&path, &run.to_le_bytes()).unwrap();
    (run, clock.now_ns().saturating_sub(started))
}

/// Charges each detector the election the library's law gives this member's group, over the round
/// trips its streams measured and the flush its disk takes.
fn elect(liveness: &mut Liveness, peers: &[u64]) {
    let (Some(granularity), Some(durable)) = (liveness.granularity(), liveness.flush_mean()) else {
        return;
    };
    let paths: Vec<_> = peers
        .iter()
        .filter_map(|peer| liveness.round_trip(*peer).copied())
        .collect();
    let Some(span) = Ballot::measure(paths.iter(), peers.len() + 1, durable, granularity)
        .and_then(|ballot| ballot.span(granularity))
    else {
        return;
    };
    for peer in peers {
        liveness.set_election(*peer, span.election).unwrap();
    }
}

/// What the member's disk is doing, as its owner knows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Disk {
    Running,
    /// Told to stop, the stop queued for the device thread behind any flush in flight.
    Asked,
    /// Told to stop, and the device thread's queue full: the stop was not taken, which its room
    /// for every request outstanding rules out.
    Refused,
    /// The device thread said it stopped.
    Stopped,
}

impl Disk {
    fn letter(self) -> char {
        match self {
            Self::Running => 'R',
            Self::Asked => 'A',
            Self::Refused => 'X',
            Self::Stopped => 'S',
        }
    }
}

/// The member's own state beside its stream's, for its report.
struct Member {
    me: u64,
    disk: Disk,
    /// When the flush in flight was asked for, on the host clock.
    flight: Option<u64>,
    /// The longest write its disk has taken, its run's record's or a flush, nanoseconds.
    flush_most: u64,
    /// The time its writes took, all told, nanoseconds: time its heartbeats waited on its device.
    blocked: u64,
    /// Whether a flush completed since its last turn.
    flushed: bool,
}

/// A state line with each change, at each flush completed (what the write took), and otherwise
/// once the member's shortest interval has passed since the last (its floor before any pair has
/// one), the soonest its evidence can move again; the supervisor waits on what they say. The
/// member's record since its last state line goes out before it, in the same write, so the counts
/// a line states follow every entry they count. A member whose thread runs states at least once a
/// period; one that states nothing is in a write, held, or not scheduled.
#[allow(
    clippy::too_many_arguments,
    reason = "the member's state, its stream, its record and its output, as the loop holds them"
)]
fn report(
    member: &Member,
    liveness: &Liveness,
    peers: &[u64],
    changes: &[Change],
    record: &mut Record,
    now: u64,
    reported_at: &mut u64,
    out: &mut impl std::io::Write,
) -> std::io::Result<()> {
    let nanos = |d: Duration| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
    let me = member.me;
    let period = peers
        .iter()
        .filter_map(|peer| liveness.report(*peer).and_then(|r| r.interval))
        .min()
        .or_else(|| liveness.floor())
        .map_or(0, nanos);
    if changes.is_empty() && !member.flushed && now < reported_at.saturating_add(period) {
        return out.flush();
    }
    *reported_at = now;
    let mut text = String::new();
    record_lines(me, &record.entries, liveness, &mut text);
    record.entries.clear();
    let mut line = format!(
        "state {me} {now} {} {} {} {} {} {}",
        member.disk.letter(),
        member.flight.unwrap_or(0),
        liveness.floor().map_or(0, nanos),
        member.flush_most,
        member.blocked,
        nanos(liveness.latest_wake(now)),
    );
    for peer in peers {
        let report = liveness.report(*peer).unwrap_or_default();
        let (trust, until) = match liveness.trust(*peer) {
            Some(Trust::Trusted { until_ns }) => ('T', until_ns),
            Some(Trust::Suspected) => ('S', 0),
            _ => ('U', 0),
        };
        line.push_str(&format!(
            " {peer}:{trust}:{}:{}:{}:{}:{}:{}:{}:{}:{until}:{}:{}",
            u8::from(report.configured),
            u8::from(report.judged),
            report.suspicions,
            report.allowance,
            report.taken,
            report.sent,
            report.interval.map_or(0, nanos),
            report.freshness.map_or(0, nanos),
            report.skipped,
            report.unproven,
        ));
    }
    text.push_str(&line);
    text.push('\n');
    out.write_all(text.as_bytes())?;
    out.flush()
}

/// The member processes, killed when the supervisor ends however it ends.
struct Members(BTreeMap<u64, Child>);

impl Members {
    /// Every member killed and reaped: a dropped `Child` leaves its process running.
    fn stop(&mut self) {
        for child in self.0.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.0.clear();
    }
}

impl Drop for Members {
    fn drop(&mut self) {
        self.stop();
    }
}

/// What a member last stated of one peer.
#[derive(Clone, Copy, Debug, Default)]
struct Seen {
    trust: char,
    configured: bool,
    judged: bool,
    suspicions: u64,
    allowance: f64,
    taken: u64,
    sent: u64,
    /// The interval the peer's heartbeats come at, and `η + α` while a margin judges, nanoseconds.
    interval: u64,
    freshness: u64,
    /// The freshness point the member trusts the peer to, while it does.
    until: u64,
    /// The slots it skipped sending, and the heartbeats it refused for their proof.
    skipped: u64,
    unproven: u64,
}

/// A member's latest state line.
#[derive(Clone, Debug, Default)]
struct Stated {
    /// When the member wrote it, on the host clock.
    at: u64,
    /// Its disk (`Disk::letter`), and when the flush in flight was asked for (zero for none), on
    /// the host clock.
    disk: char,
    flight: u64,
    /// Its floor `E[flush] + G`, its longest write, the time its writes took all told, and the
    /// latest its wakes came past what they asked, nanoseconds.
    floor: u64,
    flush_most: u64,
    blocked: u64,
    late: u64,
    peers: BTreeMap<u64, Seen>,
}

/// A suspicion a member reported.
#[derive(Clone, Copy, Debug)]
struct Suspected {
    at: u64,
    due: u64,
    sent: u64,
    detection: u64,
    /// When the member's poll noticed it.
    noticed: u64,
    /// The latest the member had woken past a wake it asked, when it noticed.
    late: u64,
}

enum Line {
    State(u64, Stated),
    /// An entry of the member's record, and for a suspicion the latest the member had woken past
    /// a wake it asked when it noticed it.
    Record(u64, Entry, Option<u64>),
    Heard(u64),
    /// A member held, and when it said so on the host clock.
    Held(u64, u64),
}

fn parse(line: &str) -> Option<Line> {
    let mut fields = line.split(' ');
    match fields.next()? {
        "state" => {
            let member = fields.next()?.parse().ok()?;
            let mut stated = Stated {
                at: fields.next()?.parse().ok()?,
                disk: fields.next()?.chars().next()?,
                flight: fields.next()?.parse().ok()?,
                floor: fields.next()?.parse().ok()?,
                flush_most: fields.next()?.parse().ok()?,
                blocked: fields.next()?.parse().ok()?,
                late: fields.next()?.parse().ok()?,
                peers: BTreeMap::new(),
            };
            for field in fields {
                let parts: Vec<&str> = field.split(':').collect();
                let [
                    peer,
                    trust,
                    configured,
                    judged,
                    suspicions,
                    allowance,
                    taken,
                    sent,
                    interval,
                    freshness,
                    until,
                    skipped,
                    unproven,
                ] = parts[..]
                else {
                    return None;
                };
                stated.peers.insert(
                    peer.parse().ok()?,
                    Seen {
                        trust: trust.chars().next()?,
                        configured: configured == "1",
                        judged: judged == "1",
                        suspicions: suspicions.parse().ok()?,
                        allowance: allowance.parse().ok()?,
                        taken: taken.parse().ok()?,
                        sent: sent.parse().ok()?,
                        interval: interval.parse().ok()?,
                        freshness: freshness.parse().ok()?,
                        until: until.parse().ok()?,
                        skipped: skipped.parse().ok()?,
                        unproven: unproven.parse().ok()?,
                    },
                );
            }
            Some(Line::State(member, stated))
        }
        "r" => {
            let rest: Vec<&str> = fields.collect();
            let (member, entry, late) = parse_record(&rest)?;
            Some(Line::Record(member, entry, late))
        }
        "heard" => Some(Line::Heard(fields.next()?.parse().ok()?)),
        "held" => Some(Line::Held(
            fields.next()?.parse().ok()?,
            fields.next()?.parse().ok()?,
        )),
        _ => None,
    }
}

/// RFC 6298 §2.1 and §2.4: the retransmission timeout before any round trip is measured, and the
/// least it is ever set to after, one second: the quiet period before any member has stated a law,
/// and how long past its due a member's line may be before a check calls it unheard, as an ask the
/// E2E harnesses wait that long for its answer (`hyper_raft_e2e::quiet`).
const RTO: Duration = Duration::from_secs(1);

/// The nanoseconds of `d`, the most a `u64` holds where it holds no more.
fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// Why a wait gave up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stuck {
    /// Nothing moved for the quiet period, at a check that heard every member.
    Quiet(Duration),
    /// A member stated nothing for `silence` past its due, past the `excuse` the members' longest
    /// write and the quiet period make.
    Silent {
        member: u64,
        silence: Duration,
        excuse: Duration,
    },
}

impl std::fmt::Display for Stuck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Quiet(period) => write!(f, "nothing moved for {period:?}"),
            Self::Silent {
                member,
                silence,
                excuse,
            } => write!(
                f,
                "member {member} stated nothing for {silence:?} past its due, past the {excuse:?} \
                 the members' longest write and the quiet period excuse"
            ),
        }
    }
}

/// What a check leaves a wait to do: look again at a time (or at a line that comes first), or wait
/// on to a quiet period's end the members' writes extended.
#[derive(Clone, Copy, Debug)]
enum Next {
    Look(u64),
    Extended(u64),
}

/// What the waits' checks have seen, for the dumps.
#[derive(Clone, Copy, Debug, Default)]
struct Checks {
    /// Checks that did not hear every member.
    unheard: u64,
    /// The longest a check found a member silent past its due, and what was excused then.
    silence_most: Duration,
    excused_then: Duration,
    /// The time waits were extended for members' writes, all told, and the most at one check.
    extended_ns: u64,
    extended_most_ns: u64,
    /// The longest the supervisor itself was deaf at once.
    deaf_most_ns: u64,
}

/// When a member's next line is due, nanoseconds after its latest: once its statement period, its
/// shortest interval (its floor before any pair has one), has passed, and its wakes' latest
/// lateness past that. A member whose thread runs states at least so often.
fn due_after(stated: &Stated) -> u64 {
    let period = stated
        .peers
        .values()
        .map(|seen| seen.interval)
        .filter(|interval| *interval > 0)
        .min()
        .unwrap_or(stated.floor);
    period.saturating_add(stated.late)
}

struct Supervisor {
    /// The member processes still running, killed when the supervisor ends however it ends.
    members: Members,
    lines: std::sync::mpsc::Receiver<String>,
    /// The host's monotonic clock, which every member's lines are stated on.
    clock: Clock,
    /// Every member line echoed to stderr (`HYPER_LIVENESS_TRACE`), for a run to be read whole.
    trace: bool,
    latest: BTreeMap<u64, Stated>,
    suspicions: Vec<(u64, u64, Suspected)>,
    /// Each member's record, as far as it stated it whole: the entries it wrote with a state line.
    records: BTreeMap<u64, Record>,
    /// Each member's entries since its latest state line, which come in with the next.
    pending: BTreeMap<u64, Vec<(Entry, Option<u64>)>>,
    /// The members that said they heard every peer and sent to each.
    heard: Vec<u64>,
    /// The first state line in which each member stated each pair configured: `(member, peer)` to
    /// its time on the host clock, which is no earlier than the configuration.
    configured_since: BTreeMap<(u64, u64), u64>,
    /// What the checks have seen, and why the latest wait that gave up did.
    checks: Checks,
    stuck: Option<Stuck>,
    /// The member that last said it is held, and when.
    held: Option<(u64, u64)>,
    /// The supervisor's own deaf time, oldest first: each span from when a wait for a line ended
    /// (or the time the wait asked to end, if it woke past it) to when the next wait began. Lines
    /// that come meanwhile wait unread, so it is no member's silence. Kept back to the earliest due
    /// of a running member's latest line, one span a wait for a line since: a wait that judges a
    /// member ends once the member is silent past its excuse.
    deaf: std::collections::VecDeque<(u64, u64)>,
    /// When the supervisor's latest wait for a line ended: it has been deaf since.
    deaf_from: u64,
}

impl Supervisor {
    /// The next line any member reports within `left`, folded in: the member that stated it;
    /// nothing, past it. The supervisor listens only while it waits here: from when its latest
    /// wait ended to when this one begins it was deaf, and so it was past the time this one asked
    /// to end, when it was not scheduled.
    fn next(&mut self, left: Duration, what: &str) -> Option<u64> {
        let began = self.clock.now_ns();
        self.deaf.push_back((self.deaf_from, began));
        self.checks.deaf_most_ns = self
            .checks
            .deaf_most_ns
            .max(began.saturating_sub(self.deaf_from));
        let received = self.lines.recv_timeout(left);
        let ended = self.clock.now_ns();
        self.deaf_from = ended.min(began.saturating_add(nanos(left)));
        self.forget_deaf();
        let line = match received {
            Ok(line) => line,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return None,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("{what}: every member stopped reporting\n{}", self.dump())
            }
        };
        Some(self.fold(&line))
    }

    /// Every line already come, folded in: the members that stated one. A check judges what the
    /// members wrote before it, not what waits in the supervisor's own queue.
    fn drain(&mut self) -> Vec<u64> {
        let mut stated = Vec::new();
        while let Ok(line) = self.lines.try_recv() {
            stated.push(self.fold(&line));
        }
        stated
    }

    /// The time in `(from, now]` the supervisor was deaf: lines that came then waited unread.
    fn deaf_since(&self, from: u64, now: u64) -> u64 {
        self.deaf
            .iter()
            .chain(std::iter::once(&(self.deaf_from, now)))
            .map(|(start, end)| end.saturating_sub((*start).max(from)))
            .fold(0, u64::saturating_add)
    }

    /// How long by `now` the supervisor has listened for a member's line past its due
    /// ([`due_after`] its latest), its own deaf time not counted: the member's silence, as an E2E
    /// harness counts it only over asks it waited on, and charges the member nothing for its own.
    fn listened(&self, stated: &Stated, now: u64) -> u64 {
        let due = stated.at.saturating_add(due_after(stated));
        now.saturating_sub(due)
            .saturating_sub(self.deaf_since(due, now))
    }

    /// Forgets the deaf spans that ended before the earliest due of a running member's latest
    /// line, once every running member has stated one: no silence is counted from before it.
    fn forget_deaf(&mut self) {
        let mut earliest = u64::MAX;
        for member in self.members.0.keys() {
            let Some(stated) = self.latest.get(member) else {
                return;
            };
            earliest = earliest.min(stated.at.saturating_add(due_after(stated)));
        }
        while self.deaf.front().is_some_and(|(_, end)| *end <= earliest) {
            self.deaf.pop_front();
        }
    }

    /// Folds `line` in; the member that stated it (zero for a line that does not parse).
    fn fold(&mut self, line: &str) -> u64 {
        if self.trace {
            eprintln!("{line}");
        }
        match parse(line) {
            Some(Line::State(member, stated)) => {
                self.recorded(member, &stated);
                for (peer, seen) in &stated.peers {
                    if seen.configured {
                        self.configured_since
                            .entry((member, *peer))
                            .or_insert(stated.at);
                    }
                }
                self.latest.insert(member, stated);
                member
            }
            Some(Line::Record(member, entry, late)) => {
                self.pending.entry(member).or_default().push((entry, late));
                member
            }
            Some(Line::Heard(member)) => {
                self.heard.push(member);
                member
            }
            Some(Line::Held(member, at)) => {
                self.held = Some((member, at));
                member
            }
            None => 0,
        }
    }

    /// `member`'s entries since its last state line, which `stated` follows in the same write,
    /// taken into its record; each count `stated` reports of a peer must be the record's.
    fn recorded(&mut self, member: u64, stated: &Stated) {
        let record = self.records.entry(member).or_default();
        for (entry, late) in self.pending.remove(&member).unwrap_or_default() {
            if let (Entry::Told(Change::Suspected(suspicion)), Some(late)) = (entry, late) {
                self.suspicions.push((
                    member,
                    suspicion.peer,
                    Suspected {
                        at: suspicion.at_ns,
                        due: suspicion.last.map_or(0, |last| last.due_ns),
                        sent: suspicion.last.map_or(0, |last| last.sent_ns),
                        detection: suspicion.detection.map_or(0, nanos),
                        noticed: suspicion.noticed_ns,
                        late,
                    },
                ));
            }
            record.push(entry);
        }
        let differs: Vec<String> = stated
            .peers
            .iter()
            .filter_map(|(peer, seen)| {
                record.differs(
                    *peer,
                    &PairReport {
                        suspicions: seen.suspicions,
                        taken: seen.taken,
                        unproven: seen.unproven,
                        sent: seen.sent,
                        skipped: seen.skipped,
                        ..PairReport::default()
                    },
                )
            })
            .collect();
        assert!(
            differs.is_empty(),
            "member {member}'s line at {}: {}\n{}",
            stated.at,
            differs.join("; "),
            self.dump()
        );
    }

    /// Every line the members wrote before they ended, folded in: once every member is stopped,
    /// their outputs end.
    fn finish(&mut self) {
        while let Ok(line) = self.lines.recv() {
            self.fold(&line);
        }
    }

    /// Every suspicion in the members' records traced to the detector's rule (`record::trace`); one
    /// that does not trace fails the test, each named. What the trace found.
    fn traced(&self) -> Traced {
        let records: Vec<(PeerId, &[Entry])> = self
            .records
            .iter()
            .map(|(member, record)| (*member, record.entries.as_slice()))
            .collect();
        let (traced, failures) = record::trace(&records);
        assert!(
            failures.is_empty(),
            "{} suspicions or passed points do not trace to the detector's rule:\n{}",
            failures.len(),
            failures.join("\n")
        );
        traced
    }

    /// How long the members may go with nothing moving before a wait gives up: the longest any
    /// member's law lets its evidence go still, from its latest state. A judged pair suspects a
    /// peer gone silent within `η + α` of its last heartbeat's expected arrival, an unjudged one
    /// takes a heartbeat each interval while its peer lives; a heartbeat waits for the flush that
    /// proves it, the longest the member's disk has taken; a poll comes up to the latest its wakes
    /// were late; and a state is stated at least once a reporting period, the member's shortest
    /// interval. Never less than a retransmission timeout, the wait before any member states one.
    fn quiet(&self) -> Duration {
        let law = self
            .latest
            .values()
            .map(|stated| {
                let evidence = stated
                    .peers
                    .values()
                    .map(|seen| {
                        if seen.judged {
                            seen.freshness
                        } else {
                            seen.interval
                        }
                    })
                    .max()
                    .unwrap_or(0);
                let period = stated
                    .peers
                    .values()
                    .map(|seen| seen.interval)
                    .filter(|interval| *interval > 0)
                    .min()
                    .unwrap_or(stated.floor);
                evidence
                    .saturating_add(stated.flush_most)
                    .saturating_add(stated.late)
                    .saturating_add(period)
            })
            .max()
            .unwrap_or(0);
        Duration::from_nanos(law).max(RTO)
    }

    /// What moves the members toward a wait's fact: each peer's trust, judgement, configuration
    /// and suspicions, the heartbeats taken on pairs not yet configured (the evidence they gather
    /// toward it), each disk, and the suspicions and arrivals reported. A configured pair's
    /// heartbeats are not progress: a peer that stops sending is then suspected within the law.
    fn signature(&self) -> Vec<u64> {
        let mut out = Vec::new();
        for (member, stated) in &self.latest {
            out.extend([*member, u64::from(u32::from(stated.disk))]);
            for (peer, seen) in &stated.peers {
                out.extend([
                    *peer,
                    u64::from(u32::from(seen.trust)),
                    u64::from(seen.judged),
                    u64::from(seen.configured),
                    seen.suspicions,
                    if seen.configured { 0 } else { seen.taken },
                ]);
            }
        }
        out.extend([self.suspicions.len() as u64, self.heard.len() as u64]);
        out
    }

    /// A pair that has taken more heartbeats unconfigured than any window holds
    /// (`hyper_timing::WINDOW_LIMIT`): a link whose correlation no window of it resolves, which
    /// the crate's moves to the interval its evidence needs exist to prevent.
    fn unresolved(&self) -> Option<(u64, u64, u64)> {
        self.latest.iter().find_map(|(member, stated)| {
            stated.peers.iter().find_map(|(peer, seen)| {
                (!seen.configured && seen.taken > WINDOW_LIMIT)
                    .then_some((*member, *peer, seen.taken))
            })
        })
    }

    /// Waits until `fact` holds of what the members stated, while they move toward it, by the rule
    /// the E2E harnesses' waits keep (`hyper_raft_e2e::quiet`), in this test's terms: the members
    /// state lines, and the supervisor reads them.
    ///
    /// The wait goes on while the members' statements move (`signature`), and once a quiet period
    /// passes with nothing moved it checks, over every line already come. Quiet is only time in
    /// which the supervisor heard every member: a member's line is due once its statement period
    /// and its wakes' lateness have passed since its latest, and one past its due by a
    /// retransmission timeout, the time an E2E harness waits for an answer, is unheard: in a
    /// write, held, or not scheduled. A check while a member is unheard decides nothing, and a line
    /// from a member that was unheard counts as movement. The time the members say their writes
    /// took since the last check extends the wait by the most any one of them took: a member's
    /// heartbeats wait on its flushes, so time in them moves nothing. A member silent past its due
    /// by more than the longest write any member has stated (its run's record or a flush) and the
    /// quiet period ends the wait, named, whatever else moves, rather than waiting for good. Its
    /// silence counts only the time the supervisor listened for its lines ([`Supervisor::listened`]),
    /// as a lost ask costs an E2E harness's test and not the member: the supervisor reads every
    /// line already come before it judges, waits for a line no longer than it must listen before
    /// the first member would be silent past the excuse, and is deaf while it does anything else or
    /// wakes past the time it asked (under load in Docker's virtual machine a supervisor that
    /// counted the wall clock fell two seconds behind its members' lines and called them silent,
    /// every member's latest line that old at once). A member that has stated nothing yet has no
    /// law to bound the wait: it states once its process is scheduled, and one whose process ended
    /// fails the wait. A pair that takes more heartbeats unconfigured than any window holds fails
    /// it at once.
    fn until(&mut self, what: &str, fact: impl Fn(&Self) -> bool) -> Result<(), Stuck> {
        self.stuck = None;
        let mut seen = self.signature();
        let mut deadline = self.clock.now_ns().saturating_add(nanos(self.quiet()));
        // What each member said its writes had taken when the last check heard it, or at its
        // first line of the wait.
        let mut blocked: BTreeMap<u64, u64> = self
            .latest
            .iter()
            .map(|(member, stated)| (*member, stated.blocked))
            .collect();
        let mut unheard = self.unheard(self.clock.now_ns());
        // When the wait looks next: the quiet period's end, or a check's look again.
        let mut look = deadline;
        // A check under way: when it began, and the members whose line it awaits.
        let mut checking: Option<(u64, Vec<u64>)> = None;
        while !fact(self) {
            let now = self.clock.now_ns();
            let running: Vec<u64> = self
                .latest
                .keys()
                .copied()
                .filter(|member| self.members.0.contains_key(member))
                .collect();
            let left = look
                .saturating_sub(now)
                .min(self.silent_in(now, &running).unwrap_or(u64::MAX));
            let mut stated: Vec<u64> = if left > 0 {
                self.next(Duration::from_nanos(left), what)
                    .into_iter()
                    .collect()
            } else {
                Vec::new()
            };
            stated.extend(self.drain());
            self.unresolved_fails(what);
            // A member's first line of the wait is where its writes' time is counted from.
            for member in &stated {
                if let Some(latest) = self.latest.get(member) {
                    blocked.entry(*member).or_insert(latest.blocked);
                }
            }
            let now = self.clock.now_ns();
            let back = stated.iter().any(|member| unheard.contains(member));
            unheard = self.unheard(now);
            self.silence(now, &unheard)
                .inspect_err(|stuck| self.stuck = Some(*stuck))?;
            let signature = self.signature();
            if signature != seen || back {
                seen = signature;
                deadline = now.saturating_add(nanos(self.quiet()));
                look = deadline;
                checking = None;
                continue;
            }
            if now < deadline {
                look = deadline;
                continue;
            }
            // A check is a look: a member whose line is past its due when it begins is awaited,
            // for that line or a retransmission timeout past its due, as an ask waits for its
            // answer; the others were heard within their period. Each is awaited once, so a check
            // ends within a timeout of its beginning.
            let (began, awaited) = checking.get_or_insert_with(|| (now, self.overdue(now)));
            let began = *began;
            awaited.retain(|member| {
                self.latest.get(member).is_some_and(|stated| {
                    stated.at < began
                        && now
                            <= stated
                                .at
                                .saturating_add(due_after(stated))
                                .saturating_add(nanos(RTO))
                })
            });
            if let Some(at) = self.answer_by(awaited) {
                look = at.max(now.saturating_add(1));
                continue;
            }
            checking = None;
            match self
                .check(what, now, deadline, &unheard, &mut blocked)
                .inspect_err(|stuck| self.stuck = Some(*stuck))?
            {
                Next::Look(at) => look = at,
                Next::Extended(to) => {
                    deadline = to;
                    look = to;
                }
            }
        }
        Ok(())
    }

    /// The members up whose latest line is past its due at `now` but not yet by a retransmission
    /// timeout: those a check begun now awaits.
    fn overdue(&self, now: u64) -> Vec<u64> {
        let rto = nanos(RTO);
        self.latest
            .iter()
            .filter(|(member, _)| self.members.0.contains_key(member))
            .filter(|(_, stated)| {
                let due = stated.at.saturating_add(due_after(stated));
                due < now && now.saturating_sub(due) <= rto
            })
            .map(|(member, _)| *member)
            .collect()
    }

    /// When the first of the members in `awaited` is past its due by a retransmission timeout.
    fn answer_by(&self, awaited: &[u64]) -> Option<u64> {
        awaited
            .iter()
            .filter_map(|member| self.latest.get(member))
            .map(|stated| {
                stated
                    .at
                    .saturating_add(due_after(stated))
                    .saturating_add(nanos(RTO))
            })
            .min()
    }

    /// The members up whose latest line is past its due ([`due_after`]) by more than a
    /// retransmission timeout at `now`: the members the supervisor does not hear.
    fn unheard(&self, now: u64) -> Vec<u64> {
        let rto = nanos(RTO);
        self.latest
            .iter()
            .filter(|(member, _)| self.members.0.contains_key(member))
            .filter(|(_, stated)| {
                let due = stated.at.saturating_add(due_after(stated));
                now.saturating_sub(due) > rto
            })
            .map(|(member, _)| *member)
            .collect()
    }

    /// What the members' own measures excuse of a member's silence past its due: the longest write
    /// any member has stated, and the quiet period.
    fn excuse(&self) -> u64 {
        let write_most = self
            .latest
            .values()
            .map(|stated| stated.flush_most)
            .max()
            .unwrap_or(0);
        nanos(self.quiet()).saturating_add(write_most)
    }

    /// How much longer the supervisor must listen from `now` before the first of `members` is
    /// silent past the excuse: a wait for a line that long, if none comes, ends past it.
    fn silent_in(&self, now: u64, members: &[u64]) -> Option<u64> {
        let past = nanos(RTO).saturating_add(self.excuse()).saturating_add(1);
        members
            .iter()
            .filter_map(|member| self.latest.get(member))
            .map(|stated| past.saturating_sub(self.listened(stated, now)))
            .min()
    }

    /// Ends the wait once a member in `unheard` has been silent past its due, in time the
    /// supervisor listened, less the retransmission timeout a check waits past it, for longer than
    /// the excuse.
    fn silence(&mut self, now: u64, unheard: &[u64]) -> Result<(), Stuck> {
        let excuse = self.excuse();
        for member in unheard {
            let Some(stated) = self.latest.get(member) else {
                continue;
            };
            let silence = self.listened(stated, now).saturating_sub(nanos(RTO));
            if silence > nanos(self.checks.silence_most) {
                self.checks.silence_most = Duration::from_nanos(silence);
                self.checks.excused_then = Duration::from_nanos(excuse);
            }
            if silence > excuse {
                return Err(Stuck::Silent {
                    member: *member,
                    silence: Duration::from_nanos(silence),
                    excuse: Duration::from_nanos(excuse),
                });
            }
        }
        Ok(())
    }

    /// A check at `now`, once the quiet period passed at `deadline` with nothing moved and every
    /// line the check awaited came or timed out: when the wait looks again, or why it gives up. A
    /// check while a member is unheard, or has stated nothing yet, decides nothing: the wait listens
    /// on until the unheard member's line moves it or its silence ends it, and looks again a quiet
    /// period on at a member that has stated nothing. Otherwise the time the members' writes took
    /// since the last check extends the deadline by the most any one took.
    fn check(
        &mut self,
        what: &str,
        now: u64,
        deadline: u64,
        unheard: &[u64],
        blocked: &mut BTreeMap<u64, u64>,
    ) -> Result<Next, Stuck> {
        let unstated = self.unstated(what);
        if !unheard.is_empty() || !unstated.is_empty() {
            self.checks.unheard = self.checks.unheard.saturating_add(1);
            if !unstated.is_empty() {
                eprintln!("{what}: waiting for members {unstated:?} to state anything");
            }
            // An unheard member's line moves the wait, and its silence past the excuse ends it: the
            // wait listens for either. One that has stated nothing is looked at again a quiet
            // period on, while its process runs.
            let again = if unstated.is_empty() {
                u64::MAX
            } else {
                now.saturating_add(nanos(self.quiet()))
            };
            return Ok(Next::Look(again));
        }
        let excused = self
            .latest
            .iter()
            .map(|(member, stated)| {
                let before = blocked.insert(*member, stated.blocked);
                before.map_or(0, |before| stated.blocked.saturating_sub(before))
            })
            .max()
            .unwrap_or(0);
        self.checks.extended_ns = self.checks.extended_ns.saturating_add(excused);
        self.checks.extended_most_ns = self.checks.extended_most_ns.max(excused);
        let deadline = deadline.saturating_add(excused);
        if now < deadline {
            Ok(Next::Extended(deadline))
        } else {
            Err(Stuck::Quiet(self.quiet()))
        }
    }

    /// The members that have stated nothing yet, each waited on while its process runs: one whose
    /// process ended fails the wait.
    fn unstated(&mut self, what: &str) -> Vec<u64> {
        let unstated: Vec<u64> = self
            .members
            .0
            .keys()
            .copied()
            .filter(|id| !self.latest.contains_key(id))
            .collect();
        for id in &unstated {
            let ended = self
                .members
                .0
                .get_mut(id)
                .and_then(|child| child.try_wait().unwrap());
            if let Some(status) = ended {
                panic!(
                    "{what}: member {id} ended before it stated anything: {status}\n{}",
                    self.dump()
                );
            }
        }
        unstated
    }

    /// Fails the wait once a pair has taken more heartbeats unconfigured than any window holds.
    fn unresolved_fails(&self, what: &str) {
        if let Some((member, peer, taken)) = self.unresolved() {
            panic!(
                "{what}: member {member} took {taken} heartbeats from {peer} unconfigured, \
                 more than any window holds\n{}",
                self.dump()
            );
        }
    }

    /// Waits until `fact` holds, as [`Supervisor::until`] does, and fails the test with every
    /// member's last state if the wait gives up.
    fn wait(&mut self, what: &str, fact: impl Fn(&Self) -> bool) {
        if let Err(stuck) = self.until(what, fact) {
            panic!("{what}: {stuck}\n{}", self.dump());
        }
    }

    /// Every member's last state and the suspicions reported, for a wait that failed: why it gave
    /// up, the quiet period, what the checks saw, and each member's latest line.
    fn dump(&self) -> String {
        let now = self.clock.now_ns();
        let ms = |ns: u64| ns as f64 / 1e6;
        let mut out = match &self.stuck {
            Some(stuck) => format!("{stuck}; "),
            None => String::new(),
        };
        out.push_str(&format!(
            "quiet period {:?}; {}",
            self.quiet(),
            self.account()
        ));
        for (member, stated) in &self.latest {
            out.push_str(&format!(
                "\n  member {member}, stated {:.1} ms ago, due {:.1} ms after, listened for {:.1} ms \
                 past it: disk {} flush in flight {} floor {:.3} ms longest write {:.3} ms writes \
                 {:.1} ms all told wakes up to {:.3} ms late",
                ms(now.saturating_sub(stated.at)),
                ms(due_after(stated)),
                ms(self.listened(stated, now)),
                stated.disk,
                if stated.flight == 0 {
                    "none".to_owned()
                } else {
                    format!("for {:.1} ms", ms(now.saturating_sub(stated.flight)))
                },
                ms(stated.floor),
                ms(stated.flush_most),
                ms(stated.blocked),
                ms(stated.late),
            ));
            for (peer, seen) in &stated.peers {
                out.push_str(&format!(
                    "\n    peer {peer}: trust {} judged {} configured {} taken {} sent {} \
                     interval {:.3} ms freshness {:.3} ms trusted until {} suspicions {} \
                     allowance {:.3}",
                    seen.trust,
                    seen.judged,
                    seen.configured,
                    seen.taken,
                    seen.sent,
                    ms(seen.interval),
                    ms(seen.freshness),
                    if seen.until == 0 {
                        "-".to_owned()
                    } else {
                        format!("{:+.1} ms of the line", ms(seen.until) - ms(stated.at))
                    },
                    seen.suspicions,
                    seen.allowance,
                ));
            }
        }
        for (member, peer, suspected) in &self.suspicions {
            out.push_str(&format!(
                "\n  suspicion by {member} of {peer}: {:.1} ms ago, {:.3} ms past the last \
                 heartbeat's schedule, bound {:.3} ms",
                ms(now.saturating_sub(suspected.at)),
                ms(suspected.at.saturating_sub(suspected.due)),
                ms(suspected.detection),
            ));
        }
        out
    }

    /// What the checks have seen: the checks that did not hear every member, the longest silence
    /// past a member's due and what was excused then, how far waits were extended for the
    /// members' writes, and the longest the supervisor was deaf at once.
    fn account(&self) -> String {
        let c = self.checks;
        format!(
            "checks that did not hear every member {}, the longest silence past a due {:.1} ms \
             against {:.1} ms excused, waits extended {:.1} ms for members' writes (at most {:.1} \
             ms at once), the supervisor deaf at most {:.1} ms at once",
            c.unheard,
            c.silence_most.as_secs_f64() * 1e3,
            c.excused_then.as_secs_f64() * 1e3,
            c.extended_ns as f64 / 1e6,
            c.extended_most_ns as f64 / 1e6,
            c.deaf_most_ns as f64 / 1e6,
        )
    }

    /// Whether `member` stated, at or after `since` on the host clock, that it holds `peer`
    /// suspected: a fact about the run after `since`, whether the suspicion began before it (a
    /// live peer falsely suspected just before it stalled or died, never trusted again) or after.
    fn holds_suspected(&self, member: u64, peer: u64, since: u64) -> bool {
        self.latest.get(&member).is_some_and(|stated| {
            stated.at >= since
                && stated
                    .peers
                    .get(&peer)
                    .is_some_and(|seen| seen.trust == 'S')
        })
    }

    /// The suspicion `member` holds of `peer`: the latest it reported.
    fn suspicion(&self, member: u64, peer: u64) -> Suspected {
        self.suspicions
            .iter()
            .filter(|(m, p, _)| *m == member && *p == peer)
            .map(|(_, _, s)| *s)
            .max_by_key(|s| s.at)
            .expect("a member that holds a peer suspected reported the suspicion")
    }
}

/// A group of `nodes` member processes, started: each ready, its disk's command port known, and
/// told to begin. The directory holds their files.
struct Group {
    _directory: tempfile::TempDir,
    supervisor: Supervisor,
    /// Where each member's disk takes commands.
    wakes: BTreeMap<u64, u64>,
}

#[allow(
    clippy::disallowed_methods,
    reason = "real processes on the host's clock, threads and environment (CLAUDE.md §1a, end to end)"
)]
fn start(nodes: u64) -> Group {
    let directory = tempfile::tempdir().unwrap();
    let mut members = Members(
        (1..=nodes)
            .map(|id| {
                let child = Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "member_process",
                        "--nocapture",
                        "--test-threads=1",
                    ])
                    .env("HYPER_LIVENESS_NODE", id.to_string())
                    .env("HYPER_LIVENESS_NODES", nodes.to_string())
                    .env(
                        "HYPER_LIVENESS_FILE",
                        directory.path().join(format!("member-{id}.log")),
                    )
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap();
                (id, child)
            })
            .collect(),
    );
    let (sender, lines) = std::sync::mpsc::channel::<String>();
    for child in members.0.values_mut() {
        let stdout = child.stdout.take().unwrap();
        let sender = sender.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
    }
    drop(sender);
    // Every member ready: its sockets bound, where its disk takes commands and where it listens.
    // A member just started has no law yet to bound the wait: it is ready once its process is
    // scheduled, and one whose process ended fails it, looked at every retransmission timeout.
    let mut wakes = BTreeMap::new();
    let mut ports = BTreeMap::new();
    while wakes.len() < nodes as usize {
        let line = match lines.recv_timeout(RTO) {
            Ok(line) => line,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                for (id, child) in &mut members.0 {
                    if let Some(status) = child.try_wait().unwrap() {
                        panic!("member {id} ended before it was ready: {status}");
                    }
                }
                continue;
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("every member ended before it was ready")
            }
        };
        // libtest prints "test member_process ... " before the body runs, without a newline.
        if let Some((_, ready)) = line.split_once("ready ") {
            let fields: Vec<u64> = ready.split(' ').map(|f| f.parse().unwrap()).collect();
            wakes.insert(fields[0], fields[1]);
            ports.insert(fields[0], fields[2]);
        }
    }
    let ports = ports
        .values()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    for child in members.0.values_mut() {
        writeln!(child.stdin.as_mut().unwrap(), "start {ports}").unwrap();
    }
    let clock = Clock::new().unwrap();
    let deaf_from = clock.now_ns();
    Group {
        _directory: directory,
        supervisor: Supervisor {
            members,
            lines,
            clock,
            trace: std::env::var_os("HYPER_LIVENESS_TRACE").is_some(),
            latest: BTreeMap::new(),
            suspicions: Vec::new(),
            records: BTreeMap::new(),
            pending: BTreeMap::new(),
            heard: Vec::new(),
            configured_since: BTreeMap::new(),
            checks: Checks::default(),
            stuck: None,
            held: None,
            deaf: std::collections::VecDeque::new(),
            deaf_from,
        },
        wakes,
    }
}

#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "real processes on the host's clock, threads and environment (CLAUDE.md §1a, end to end)"
)]
fn a_stalled_disk_and_a_killed_node_are_suspected_and_no_live_one_is() {
    if std::env::var("HYPER_LIVENESS_NODE").is_ok() {
        return;
    }
    let Group {
        _directory,
        mut supervisor,
        wakes,
    } = start(NODES);
    let clock = Clock::new().unwrap();

    // Every pair configured.
    supervisor.wait("every pair configured", |s| {
        s.latest.len() == NODES as usize
            && s.latest.values().all(|stated| {
                stated.peers.len() == NODES as usize - 1
                    && stated.peers.values().all(|p| p.configured)
            })
    });

    // A disk that stops completing flushes.
    let stalled_at = clock.now_ns();
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .send_to(b"stall", ("127.0.0.1", wakes[&STALLED] as u16))
        .unwrap();
    let watchers: Vec<u64> = (1..=NODES).filter(|m| *m != STALLED).collect();
    supervisor.wait("every other member suspects the stalled disk", |s| {
        watchers
            .iter()
            .all(|m| s.holds_suspected(*m, STALLED, stalled_at))
    });
    for member in &watchers {
        let found = supervisor.suspicion(*member, STALLED);
        assert!(found.detection > 0, "member {member}: a bound is stated");
        assert!(
            found.at - found.due <= found.detection,
            "member {member} suspected the stalled disk {} ns after its last heartbeat was due, \
             past its stated bound of {} ns",
            found.at - found.due,
            found.detection
        );
        assert!(found.sent >= found.due);
    }

    // A node killed.
    let mut victim = supervisor.members.0.remove(&KILLED).unwrap();
    let killed_at = clock.now_ns();
    victim.kill().unwrap();
    victim.wait().unwrap();
    let survivors: Vec<u64> = (1..=NODES).filter(|m| *m != KILLED).collect();
    supervisor.wait("every survivor suspects the killed member", |s| {
        survivors
            .iter()
            .all(|m| s.holds_suspected(*m, KILLED, killed_at))
    });
    for member in &survivors {
        let found = supervisor.suspicion(*member, KILLED);
        assert!(
            found.at - found.due <= found.detection,
            "member {member} suspected the killed node {} ns after its last heartbeat was due, \
             past its stated bound of {} ns",
            found.at - found.due,
            found.detection
        );
    }
    // Live members: the pairs between the members that neither stalled nor died, both ways, as
    // each reports once it holds both of the others suspected.
    let live: Vec<u64> = (1..=NODES)
        .filter(|m| *m != STALLED && *m != KILLED)
        .collect();
    supervisor.wait("each live member states both suspected", |s| {
        live.iter().all(|m| {
            s.holds_suspected(*m, STALLED, killed_at) && s.holds_suspected(*m, KILLED, killed_at)
        })
    });
    supervisor.members.stop();
    supervisor.finish();
    let traced = supervisor.traced();

    let (mut suspicions, mut allowance) = (0u64, 0.0f64);
    for member in &live {
        for peer in live.iter().filter(|p| *p != member) {
            // Whether a live peer is trusted at this moment is no promise; the count is.
            let seen = supervisor.latest[member].peers[peer];
            suspicions += seen.suspicions;
            allowance += seen.allowance;
        }
    }
    // Every suspicion each member made traced to the detector's rule; the count against the
    // allowance is the model's figure, reported, not asserted.
    println!("every suspicion traced: {traced}");
    println!(
        "stalled disk suspected after {:?}; killed node after {:?}; suspicions of live members \
         {suspicions}, allowance {allowance:.3}",
        watchers
            .iter()
            .map(|m| {
                let s = supervisor.suspicion(*m, STALLED);
                (
                    m,
                    Duration::from_nanos(s.at - s.due),
                    Duration::from_nanos(s.detection),
                )
            })
            .collect::<Vec<_>>(),
        survivors
            .iter()
            .map(|m| {
                let s = supervisor.suspicion(*m, KILLED);
                (
                    m,
                    Duration::from_nanos(s.at - s.due),
                    Duration::from_nanos(s.detection),
                )
            })
            .collect::<Vec<_>>(),
    );
}

/// Members of the group whose victim dies young: the fewest that elect without one of them, so each
/// survivor has one live link, whose configuration lengthens its interval and with it the rate its
/// node's pool is fed at (`docs/timing.md` §3, item 10).
const YOUNG_NODES: u64 = 3;

/// A member killed in its links' first heartbeats, once it has heard every peer and sent to each,
/// before any pair could have its own evidence (a configuration needs an Allan level of seven
/// windows, 56 heartbeats at the least): every survivor suspects it, within the bound it states
/// where it states one, and no later than the first poll once both its freshness point has passed
/// and a pair of its own has configured, the evidence the young link is judged by
/// (`docs/timing.md` §2.8, "Judged before its own evidence").
#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "real processes on the host's clock, threads and environment (CLAUDE.md §1a, end to end)"
)]
fn a_node_killed_in_its_first_heartbeats_is_suspected_once_a_sibling_has_its_evidence() {
    if std::env::var("HYPER_LIVENESS_NODE").is_ok() {
        return;
    }
    let victim = YOUNG_NODES;
    let Group {
        _directory,
        mut supervisor,
        ..
    } = start(YOUNG_NODES);
    let clock = Clock::new().unwrap();
    supervisor.wait("the victim heard every peer", |s| s.heard.contains(&victim));
    let mut child = supervisor.members.0.remove(&victim).unwrap();
    let killed_at = clock.now_ns();
    child.kill().unwrap();
    child.wait().unwrap();
    let survivors: Vec<u64> = (1..YOUNG_NODES).collect();
    supervisor.wait("every survivor suspects the young victim", |s| {
        survivors
            .iter()
            .all(|m| s.holds_suspected(*m, victim, killed_at))
    });
    supervisor.members.stop();
    supervisor.finish();
    println!("every suspicion traced: {}", supervisor.traced());
    let mut noticed = Vec::new();
    for member in &survivors {
        let found = supervisor.suspicion(*member, victim);
        if found.due > 0 && found.detection > 0 {
            assert!(
                found.at - found.due <= found.detection,
                "member {member} suspected the young victim {} ns after its last heartbeat was \
                 due, past its stated bound of {} ns",
                found.at - found.due,
                found.detection
            );
        }
        let sibling = survivors.iter().copied().find(|m| m != member).unwrap();
        let evidence = supervisor
            .configured_since
            .get(&(*member, sibling))
            .copied()
            .unwrap_or(u64::MAX);
        assert!(
            found.noticed <= (found.at + found.late).max(evidence),
            "member {member} noticed the young victim's death {:?} after the kill: its freshness \
             point {:?} after it, its wakes up to {:?} late, its sibling configured {:?} after it",
            Duration::from_nanos(found.noticed.saturating_sub(killed_at)),
            Duration::from_nanos(found.at.saturating_sub(killed_at)),
            Duration::from_nanos(found.late),
            Duration::from_nanos(evidence.saturating_sub(killed_at)),
        );
        noticed.push((
            member,
            Duration::from_nanos(found.noticed.saturating_sub(killed_at)),
        ));
    }
    println!("a node killed in its first heartbeats was noticed dead after {noticed:?}");
}

/// Members of the group one of which the supervisor stops: the fewest that judge a member by the
/// others' word, as the young victim's group.
const STOPPED_NODES: u64 = 3;

impl Supervisor {
    /// Stops member `id` without ending it: it stays up and states nothing, as a member deadlocked
    /// does. `SIGSTOP` on Unix, then the system's word that the process stopped (`ps`'s state
    /// `T`), the signal taking effect when the process next enters its kernel; when, on the host
    /// clock, the stop was a fact: no line of the member's is stamped later.
    #[cfg(unix)]
    fn stop_member(&mut self, id: u64, _wake: u16, what: &str) -> u64 {
        self.signal(id, "-STOP");
        let pid = self.members.0[&id].id().to_string();
        loop {
            let state = Command::new("ps")
                .args(["-o", "stat=", "-p", &pid])
                .output()
                .unwrap();
            if String::from_utf8_lossy(&state.stdout)
                .trim_start()
                .starts_with('T')
            {
                return self.clock.now_ns();
            }
            // Not stopped yet: a line meanwhile, or a retransmission timeout, and the system is
            // asked again, while the process runs.
            if self.next(RTO, what).is_none() {
                let ended = self
                    .members
                    .0
                    .get_mut(&id)
                    .and_then(|child| child.try_wait().unwrap());
                assert!(ended.is_none(), "{what}: member {id} ended");
            }
        }
    }

    /// Windows has no signal that stops a process: the member is told on its wake socket to hold
    /// its thread until a byte comes on its standard input, and says when it holds; when it said
    /// so: no line of the member's is stamped later.
    #[cfg(windows)]
    #[allow(
        clippy::disallowed_methods,
        reason = "real processes on the host's clock, threads and environment (CLAUDE.md §1a, end to end)"
    )]
    fn stop_member(&mut self, id: u64, wake: u16, what: &str) -> u64 {
        self.held = None;
        UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .send_to(HOLD, ("127.0.0.1", wake))
            .unwrap();
        loop {
            if let Some((member, at)) = self.held
                && member == id
            {
                return at;
            }
            if self.next(RTO, what).is_none() {
                let ended = self
                    .members
                    .0
                    .get_mut(&id)
                    .and_then(|child| child.try_wait().unwrap());
                assert!(ended.is_none(), "{what}: member {id} ended");
            }
        }
    }

    /// Lets member `id` go on after [`Supervisor::stop_member`], and waits, while its process runs,
    /// for its first line since: what it stated before it stopped is no word of it since, and the
    /// silence the supervisor ordered is not the member's (as hyper-raft-e2e's `thaw`).
    fn release_member(&mut self, id: u64, what: &str) {
        let released = self.clock.now_ns();
        #[cfg(unix)]
        self.signal(id, "-CONT");
        #[cfg(windows)]
        {
            let child = self.members.0.get_mut(&id).unwrap();
            let stdin = child.stdin.as_mut().unwrap();
            stdin.write_all(&[1]).unwrap();
            stdin.flush().unwrap();
        }
        while !self
            .latest
            .get(&id)
            .is_some_and(|stated| stated.at > released)
        {
            if self.next(RTO, what).is_none() {
                let ended = self
                    .members
                    .0
                    .get_mut(&id)
                    .and_then(|child| child.try_wait().unwrap());
                assert!(ended.is_none(), "{what}: member {id} ended while stopped");
            }
        }
    }

    /// Sends `signal` to member `id`'s process with the system's `kill`.
    #[cfg(unix)]
    fn signal(&mut self, id: u64, signal: &str) {
        let pid = self.members.0[&id].id();
        let status = Command::new("kill")
            .args([signal, &pid.to_string()])
            .status()
            .unwrap();
        assert!(status.success(), "kill {signal} {pid} failed: {status}");
    }
}

/// A member stopped (`SIGSTOP`; on Windows, which has no signal that stops a process, it holds its
/// thread until a byte comes on its standard input), once every pair is configured: the wait for a
/// line only it can state fails, naming it, once its silence past its due passes what the members'
/// longest write and the quiet period excuse, rather than waiting for good. Let go, it states
/// again, and every other member states it trusted.
#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "real processes on the host's clock, threads and environment (CLAUDE.md §1a, end to end)"
)]
fn a_stopped_member_fails_the_wait_that_needs_it_by_name() {
    if std::env::var("HYPER_LIVENESS_NODE").is_ok() {
        return;
    }
    let Group {
        _directory,
        mut supervisor,
        wakes,
    } = start(STOPPED_NODES);
    let clock = Clock::new().unwrap();
    supervisor.wait("every pair configured", |s| {
        s.latest.len() == STOPPED_NODES as usize
            && s.latest.values().all(|stated| {
                stated.peers.len() == STOPPED_NODES as usize - 1
                    && stated.peers.values().all(|p| p.configured)
            })
    });
    let stopped = STOPPED_NODES;
    let stopped_at = supervisor.stop_member(stopped, wakes[&stopped] as u16, "the member stops");
    let outcome = supervisor.until("the stopped member states again", |s| {
        s.latest
            .get(&stopped)
            .is_some_and(|stated| stated.at > stopped_at)
    });
    let Err(Stuck::Silent {
        member,
        silence,
        excuse,
    }) = outcome
    else {
        panic!(
            "the wait for the stopped member ended {outcome:?}, not naming it silent\n{}",
            supervisor.dump()
        );
    };
    assert_eq!(
        member, stopped,
        "the wait named member {member}, not member {stopped}, which was stopped"
    );
    let released_at = clock.now_ns();
    supervisor.release_member(stopped, "the released member states again");
    let others: Vec<u64> = (1..STOPPED_NODES).collect();
    supervisor.wait("the released member is trusted", |s| {
        others.iter().all(|m| {
            s.latest.get(m).is_some_and(|stated| {
                stated.at > released_at
                    && stated.peers.get(&stopped).is_some_and(|p| p.trust == 'T')
            })
        })
    });
    supervisor.members.stop();
    supervisor.finish();
    println!("every suspicion traced: {}", supervisor.traced());
    println!(
        "a member stopped: the wait for it failed after {silence:?} of its silence past its due \
         against {excuse:?} excused, naming it; let go, it was trusted again; {}",
        supervisor.account()
    );
}
