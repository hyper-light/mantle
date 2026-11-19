//! DIAGNOSTIC ONLY, never landed (branches diag-doors, repro/trace, repro/trace-2): a stall trace.
//! Each request's stages through both doors, the host's asks and answers, a native put's body
//! piece by piece, its room and its window, and the node loop's turns, stamped on CLOCK_MONOTONIC
//! (the clock `perf record -k CLOCK_MONOTONIC` and the harness stamp with), so a slow request's
//! time is attributed to the stage it waited in. Compiled only with this crate's `diag-trace`
//! feature (mantle's `diag-trace`), so the default build has none of it; on only where
//! `MANTLE_DIAG_TRACE` names a directory; written there at each `counts` answer, or each beat by a
//! process that writes it as it goes ([`write_as_it_goes`]) (`trace-<pid>-<n>.txt`).
//!
//! A window's file: its head line, `k <id> <what> <key>` for each request's key, and
//! `e <ns> <id> <stage> <a> <b>` for each event, `b` 0 where its stage states one value.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]

use std::cell::Cell;
use std::io::Write;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

/// Events a window holds at most; past it they are counted as dropped.
const MOST: usize = 4_000_000;

/// The gap, on the stamping clock, between two stamps of a state a job holds at every turn
/// (`J_HELD`, `N_VOUCHED`): 100 ms, the node's turn in the stall CI caught, chosen so a quiet
/// job's turns each show and a busy node's thousands a second do not bury its other stages.
pub const SAMPLE_NS: u64 = 100_000_000;

pub const ACCEPT: u16 = 1;
pub const H_HEAD: u16 = 2;
pub const H_AUTH: u16 = 3;
pub const H_SUBMIT: u16 = 4;
pub const H_READY: u16 = 5;
pub const H_BODY_SENT: u16 = 6;
pub const H_REPLY: u16 = 7;
pub const H_RESPONDED: u16 = 8;
pub const H_END: u16 = 9;
pub const N_ARRIVED: u16 = 20;
pub const N_DECODED: u16 = 21;
pub const J_TAKEN: u16 = 30;
pub const J_ASK: u16 = 31;
pub const J_DONE: u16 = 32;
pub const J_REPLY: u16 = 33;
pub const J_END: u16 = 34;
pub const J_OVER: u16 = 35;
pub const T_BEGIN: u16 = 40;
pub const T_PHASE: u16 = 41;
pub const T_END: u16 = 42;
pub const C_INTENDED: u16 = 50;
pub const C_SEND: u16 = 51;
pub const C_DONE: u16 = 52;

// repro/trace-2: a native put's life between its asks. Each names its stage's `a` and `b`.
/// The node's task read a piece of a put's body off its exchange: bytes, the body's bytes read
/// so far (`Node::feed_body`).
pub const N_READ: u16 = 22;
/// The node's task gave a piece to the put's job on its door's channel: bytes, bytes given so far
/// (`Feeding::send`).
pub const N_FED: u16 = 23;
/// A piece the job's channel had no room for, held: bytes, bytes given so far.
pub const N_FED_HELD: u16 = 24;
/// The body's end given to the job: bytes given in all, the trailer's bytes.
pub const N_FED_END: u16 = 25;
/// The feeding let go (`Node::feed_bodies`): bytes given in all, 1 where the job had let go of
/// the body first.
pub const N_FED_OVER: u16 = 26;
/// A parked request vouched for (`Node::take_parked`): requests parked, its place among them.
pub const N_PARKED_HELD: u16 = 27;
/// The node's task told a native job's asker its work is held (`Exchange::working`): 0, or 1 where
/// its reply waits for room in the endpoint's budget. Sampled: stamped at most once a sample's gap
/// ([`SAMPLE_NS`]), as a held job is vouched for at every turn.
pub const N_VOUCHED: u16 = 28;
/// Room at a volume or in the transport freed this turn (`Serve::freed`), on the host's shard,
/// id 0: jobs waiting for room, jobs.
pub const N_ROOM_FREED: u16 = 29;
/// The put's job took a piece off its channel into its put (`take_body`): bytes the put took of
/// it, the body's bytes the put took so far.
pub const J_TOOK: u16 = 36;
/// An ask refused for room (`NodeError::Asks`), or a command not asked for want of it: its kind
/// (0 command, 1 read, 2 chunk), 1 where not asked for want of command room, else 0. `J_ASK` is
/// stamped where the job's ask is kept: its kind, its number (on a driving shard, the shard's).
pub const J_ASK_ROOM: u16 = 37;
/// A chunk ask answered (`taken`): its class (0 stored, 1 room, 2 refused, 3 full, 4 failed), and
/// what said so (host.rs `diag_chunk`: 0 stored, 1 a get's bytes, 10 + the volume's status, 100 +
/// the transport's refusal, each in the order `taken` lists them).
pub const J_CHUNK: u16 = 38;
/// Room offered to the job (`Host::offer_room`): why (1 room freed, 2 a beat passed, 3 both),
/// which drivers waited for it (1 put, 2 get, 3 both).
pub const J_ROOM_OFFER: u16 = 39;
/// The job held and vouched for at the turn (`Host::bound_jobs`, `Native::held`): asks out, 1
/// where a chunk of it waits for room. Stamped where they changed, else at most once a sample's gap
/// ([`SAMPLE_NS`]), as a held job is held at every turn.
pub const J_HELD: u16 = 60;
/// A native request handed to a shard (`Host::take_handed`): the shard it was placed on, 1 where
/// the node's task runs it.
pub const J_HANDED: u16 = 61;
/// A put's window as it waits, stamped where it changed since the last (`Host::step_put`): blocks
/// going down, chunk asks out.
pub const J_WINDOW: u16 = 62;
/// With it: 1 where a full block waits for a place among them, the put's phase (0 body, 1 ended, 2
/// closing, 3 file, 4 name, 5 over).
pub const J_WINDOW_WAIT: u16 = 63;
/// With it: chunks waiting for room at their volume, the driver's requests the node had no room
/// for, held in the job.
pub const J_WINDOW_ROOM: u16 = 64;
/// With it: the driver's requests asked and unanswered, those ready and not yet taken.
pub const J_WINDOW_ASKED: u16 = 65;
/// A driving shard's ask held by the node's task for want of the node's room (`Lanes::take`), id
/// 0: the shard's ask number (`J_ASK`'s `b`), the shard.
pub const L_WAIT: u16 = 70;
/// That held ask asked of the node: the shard's ask number, the shard.
pub const L_ASKED: u16 = 71;

static ON: AtomicU8 = AtomicU8::new(0);
static NEXT: AtomicU64 = AtomicU64::new(1);
static DROPPED: AtomicU64 = AtomicU64::new(0);
static WINDOW: AtomicU64 = AtomicU64::new(0);
/// An event: its time, its id, its stage and its two values.
type Event = (u64, u64, u16, u64, u64);
static LOG: Mutex<Vec<Event>> = Mutex::new(Vec::new());
static KEYS: Mutex<Vec<(u64, String)>> = Mutex::new(Vec::new());

thread_local! {
    /// The client's request under way on this thread, for the key its operation names; in a node,
    /// the host's job under way, which its asks (`Serve::ask`) and its body's taking stamp.
    static CURRENT: Cell<u64> = const { Cell::new(0) };
}

/// Whether the trace is on.
pub fn on() -> bool {
    match ON.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = std::env::var_os("MANTLE_DIAG_TRACE").is_some();
            if on && let Ok(mut log) = LOG.lock() {
                log.reserve(MOST);
            }
            ON.store(if on { 2 } else { 1 }, Ordering::Relaxed);
            on
        }
    }
}

/// CLOCK_MONOTONIC, in nanoseconds.
pub fn now() -> u64 {
    nanos(rustix::time::clock_gettime(
        rustix::time::ClockId::Monotonic,
    ))
}

/// This thread's CPU time, in nanoseconds.
pub fn cpu() -> u64 {
    nanos(rustix::time::clock_gettime(
        rustix::time::ClockId::ThreadCPUTime,
    ))
}

/// A clock's reading in nanoseconds; neither clock reads a negative time.
fn nanos(t: rustix::time::Timespec) -> u64 {
    let seconds = u64::try_from(t.tv_sec).unwrap_or(0);
    let nanoseconds = u64::try_from(t.tv_nsec).unwrap_or(0);
    seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(nanoseconds)
}

/// A fresh id; 0 where the trace is off.
pub fn id() -> u64 {
    if on() {
        NEXT.fetch_add(1, Ordering::Relaxed)
    } else {
        0
    }
}

pub fn ev(id: u64, stage: u16, a: u64) {
    ev2(id, stage, a, 0);
}

/// An event of a stage that states two values.
pub fn ev2(id: u64, stage: u16, a: u64, b: u64) {
    if !on() {
        return;
    }
    let t = now();
    if let Ok(mut log) = LOG.lock() {
        if log.len() < MOST {
            log.push((t, id, stage, a, b));
        } else {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// A count as a stamp's value.
pub fn n(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// The key a request names.
pub fn key(id: u64, what: &str, key: &str) {
    if !on() {
        return;
    }
    if let Ok(mut keys) = KEYS.lock() {
        keys.push((id, format!("{what} {key}")));
    }
}

pub fn set_current(id: u64) {
    CURRENT.with(|c| c.set(id));
}
pub fn current() -> u64 {
    CURRENT.with(Cell::get)
}

/// The op's kind and key, for [`key`].
pub fn op_key(id: u64, op: &mantle_protocol::object::Op) {
    use mantle_protocol::object::Op;
    if !on() {
        return;
    }
    let (what, k) = match op {
        Op::Put { key, .. } => ("Put", key.as_str()),
        Op::Get { key, .. } => ("Get", key.as_str()),
        Op::Head { key, .. } => ("Head", key.as_str()),
        Op::Delete { key, .. } => ("Delete", key.as_str()),
        Op::CreateUpload { key, .. } => ("CreateUpload", key.as_str()),
        Op::UploadPart { key, .. } => ("UploadPart", key.as_str()),
        Op::Complete { key, .. } => ("Complete", key.as_str()),
        Op::Abort { key, .. } => ("Abort", key.as_str()),
        Op::ListParts { key, .. } => ("ListParts", key.as_str()),
        Op::Open { key, .. } => ("Open", key.as_str()),
        Op::Append { key, .. } => ("Append", key.as_str()),
        Op::Offset { key, .. } => ("Offset", key.as_str()),
        Op::Finish { key, .. } => ("Finish", key.as_str()),
        Op::List(_) => ("List", "-"),
        _ => ("Other", "-"),
    };
    key(id, what, k);
}

/// Writes the trace as the process goes, a window labelled `label` each beat, on a thread of its
/// own: for a process no harness asks `counts` of, which may end at any moment and writes nothing
/// as it ends, as the node suite's `mantle node`s, each killed with SIGKILL as a power cut takes it
/// (crates/mantle/tests/node.rs). The beat is a third of the serve period
/// (`mantle_protocol::bounds::idle_timeout`), the beat a node's notes vouch for its work at
/// (hyper-transport's `Endpoint::working`): a client cuts an exchange `Stalled` only once it moved
/// nothing for a serve period at the least, and the suite kills its nodes after that, so by the
/// kill every event of the stall but its last beat's is written. Nothing where the trace is off.
pub fn write_as_it_goes(label: String) -> std::io::Result<()> {
    if !on() {
        return Ok(());
    }
    let beat = mantle_protocol::bounds::idle_timeout() / 3;
    std::thread::Builder::new()
        .name("diag-trace".to_owned())
        .spawn(move || {
            loop {
                // The thread is the trace's alone, so its wait holds up no worker; woken early,
                // it writes the window early.
                std::thread::park_timeout(beat);
                dump(&label);
            }
        })
        .map(drop)
}

/// Writes the window's events and keys and begins the next.
pub fn dump(label: &str) {
    if !on() {
        return;
    }
    let Some(dir) = std::env::var_os("MANTLE_DIAG_TRACE") else {
        return;
    };
    let events = LOG
        .lock()
        .map(|mut l| std::mem::replace(&mut *l, Vec::with_capacity(MOST)))
        .unwrap_or_default();
    let keys = KEYS
        .lock()
        .map(|mut k| std::mem::take(&mut *k))
        .unwrap_or_default();
    let n = WINDOW.fetch_add(1, Ordering::SeqCst);
    let path = std::path::Path::new(&dir).join(format!("trace-{}-{n}.txt", std::process::id()));
    if let Ok(f) = std::fs::File::create(path) {
        let mut f = std::io::BufWriter::new(f);
        let _ = writeln!(
            f,
            "trace {n} label {label} events {} dropped {}",
            events.len(),
            DROPPED.swap(0, Ordering::SeqCst)
        );
        for (id, k) in keys {
            let _ = writeln!(f, "k {id} {k}");
        }
        for (t, id, stage, a, b) in events {
            let _ = writeln!(f, "e {t} {id} {stage} {a} {b}");
        }
        let _ = f.flush();
    }
}
