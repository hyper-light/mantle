//! The clients' history a cell's tests record and check: clients, each its own identity and
//! session, putting and reading two keys of one bucket through a range's leaders, every operation
//! recorded with its call and its return on one clock, and each key's history checked
//! linearizable by the WGL checker (`crates/range/tests/support/linear.rs`, which the test that
//! includes this file takes as `linear`). The cluster test runs its nodes as threads and the
//! process test as `mantle node` processes; both record and check their histories here.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use hyper_rt::runtime::LocalRuntime;
use mantle_meta::name::{self, GateChange, Preconditions, Put};
use mantle_meta::record::{GateState, Version, Versioning};
use mantle_meta::wire::{Answer, Command as Op, Entry, Sessioned};
use mantle_node::client::{Client, ClientSettings};
use mantle_node::read::{Query, Read};

use crate::linear::{self, Input, Operation, Output, Register, Verdict};

/// The keys the clients write and read: two, so the history's partitions are checked apart
/// (Herlihy and Wing's locality) and each key's interleaving is dense.
pub const KEYS: [&str; 2] = ["k0", "k1"];
/// The steps a key's check may take: the range simulation's (`crates/range/tests/sim.rs`).
pub const CHECK_BUDGET: u64 = 10_000_000;

/// A registration, told from the others by its serial.
pub fn registration(serial: u64) -> Entry {
    Entry {
        at_ns: 0,
        commands: vec![Sessioned {
            session: 0,
            serial,
            unanswered: 0,
            command: Op::Register,
        }],
    }
}

/// A put of `key`, its etag `etag`, the version naming `file`: as the range simulation writes one
/// (`crates/range/tests/sim.rs`).
pub fn put(key: &str, etag: &str, file: u128) -> Op {
    Op::Name(Box::new(name::Command::Put(Put {
        bucket: "b".into(),
        incarnation: 1,
        key: key.into(),
        versioning: Versioning::Enabled,
        preconditions: Preconditions::default(),
        at_ns: 0,
        ordered_ns: None,
        version: Version {
            marker: false,
            null: false,
            modified_ns: 0,
            etag: etag.into(),
            size: 1,
            checksum: None,
            file: Some(file),
            owner: "o".into(),
            headers: Vec::new(),
            retention: None,
            legal_hold: None,
            listing: None,
        },
        default: None,
        id: 0,
        deadline_ns: u64::MAX,
    })))
}

/// The gate a put needs, as the range simulation opens it.
pub fn open_gate() -> Op {
    Op::Name(Box::new(name::Command::Gate(GateChange {
        bucket: "b".into(),
        incarnation: 1,
        attempt: 1,
        from: None,
        to: Some(GateState::Open),
        generation: 1,
    })))
}

/// One command of a session: its serial, and every answer before it acknowledged.
pub fn sessioned(session: u64, serial: u64, command: Op) -> Entry {
    Entry {
        at_ns: 0,
        commands: vec![Sessioned {
            session,
            serial,
            unanswered: serial,
            command,
        }],
    }
}

/// Registers a session of `group`'s range under `nonce`: its ID, or why the range gave none.
pub async fn register(client: &mut Client, group: u128, nonce: u64) -> Result<u64, String> {
    match client.command(group, &registration(nonce), u64::MAX).await {
        Ok(answers) => match answers.as_slice() {
            [Answer::Registered { session }] => Ok(*session),
            _ => Err(format!("registration answered {answers:?}")),
        },
        Err(error) => Err(error.to_string()),
    }
}

/// Opens the bucket's gate, which every put needs, through `client`: its session registered,
/// the gate its first command. The gate's answer, or why there was none.
pub async fn open_bucket(
    client: &mut Client,
    group: u128,
    nonce: u64,
) -> Result<Vec<Answer>, String> {
    let session = register(client, group, nonce).await?;
    client
        .command(group, &sessioned(session, 1, open_gate()), u64::MAX)
        .await
        .map_err(|e| e.to_string())
}

/// A client's part of the history: each operation's key, and its call and return on the test's
/// clock with what it asked and was answered.
pub type Part = Vec<(&'static str, Operation<Input, Output>)>;

/// A workload client's operations: the range's group, how many, the phase of the history they
/// are (each phase's puts write values of their own), and the clock the history is on.
#[derive(Clone, Copy)]
pub struct Plan {
    pub group: u128,
    pub ops: u64,
    pub phase: u64,
    pub start: Instant,
}

/// What steers a workload client as it runs: where a node it had not heard of listens, and the
/// test's stop, its sender gone.
pub struct Steer {
    pub learn: mpsc::Receiver<(u64, SocketAddr)>,
    pub stop: mpsc::Receiver<()>,
}

/// What a workload client tells the test.
pub enum Told {
    /// Its operations done, of all it does, and how long it now gives an exchange before it
    /// takes the node as lost (`Client::patience`).
    Progress(u64, u64, Option<Duration>),
    /// It ended on an error.
    Failed(u64, String),
}

/// A workload client's thread: on a shard `runtime` makes for its sockets, it registers its
/// session, then `plan.ops` operations, a put and a get of each key in turn, each recorded; it
/// stops early when the test does, and takes where nodes listen as the test tells it. An
/// operation has no deadline of its own, as the client's commands have none: the client follows
/// the leader through stops and cuts, a command asked again with its serial, so the session
/// applies it once; the test's watch ends an operation that is never answered, and one refused
/// fails the test.
pub fn workload(
    settings: ClientSettings,
    bound: Vec<(u64, SocketAddr)>,
    (plan, steer): (Plan, Steer),
    runtime: fn(usize) -> LocalRuntime,
    tell: impl Fn(Told) + 'static,
) -> Part {
    let id = settings.id;
    let Plan {
        group,
        ops,
        phase,
        start,
    } = plan;
    // Its endpoint's socket and one for each node.
    runtime(settings.nodes.len() + 1)
        .block_on(async move {
            let mut part = Vec::new();
            let mut client = match Client::open(settings).await {
                Ok(client) => client,
                Err(error) => {
                    tell(Told::Failed(id, error.to_string()));
                    return part;
                }
            };
            for (node, quic) in bound {
                client.learn(node, quic).unwrap();
            }
            let session = match register(&mut client, group, id).await {
                Ok(session) => session,
                Err(why) => {
                    tell(Told::Failed(id, why));
                    return part;
                }
            };
            let mut serial = 0u64;
            for i in 0..ops {
                if matches!(steer.stop.try_recv(), Err(mpsc::TryRecvError::Disconnected)) {
                    break;
                }
                while let Ok((node, quic)) = steer.learn.try_recv() {
                    client.learn(node, quic).unwrap();
                }
                let key = KEYS[usize::try_from(i / 2).unwrap() % KEYS.len()];
                let call = since(start);
                let (input, output) = if i % 2 == 0 {
                    serial += 1;
                    let etag = format!("c{id}-{phase}-{i}");
                    let file = u128::from(id) << 64 | u128::from(phase) << 32 | u128::from(i);
                    let entry = sessioned(session, serial, put(key, &etag, file));
                    match client.command(group, &entry, u64::MAX).await {
                        Ok(answers)
                            if matches!(
                                answers.as_slice(),
                                [Answer::Name(name::Outcome::Put { .. })]
                            ) =>
                        {
                            (Input::Put(etag), Output::Put)
                        }
                        answered => {
                            tell(Told::Failed(id, format!("put {i} of {key}: {answered:?}")));
                            return part;
                        }
                    }
                } else {
                    let query = Query::Current {
                        bucket: "b".into(),
                        key: key.into(),
                    };
                    match client.read(group, &query, u64::MAX).await {
                        Ok(Read::Current(current)) => {
                            (Input::Get, Output::Got(current.map(|(_, v)| v.etag)))
                        }
                        Err(error) => {
                            tell(Told::Failed(id, format!("read {i} of {key}: {error}")));
                            return part;
                        }
                    }
                };
                part.push((
                    key,
                    Operation {
                        call,
                        ret: Some(since(start)),
                        input,
                        output: Some(output),
                    },
                ));
                tell(Told::Progress(id, i + 1, client.patience()));
            }
            part
        })
        .unwrap()
}

/// Nanoseconds since `start`: the clock every client's history is on.
fn since(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap()
}

/// Checks the history's every key linearizable, and that some read of each saw a put.
pub fn check_histories(history: impl Iterator<Item = (&'static str, Operation<Input, Output>)>) {
    let mut by_key: BTreeMap<&str, Vec<Operation<Input, Output>>> = BTreeMap::new();
    for (key, op) in history {
        by_key.entry(key).or_default().push(op);
    }
    for (key, ops) in &by_key {
        let read = ops
            .iter()
            .filter(|o| matches!(o.output, Some(Output::Got(Some(_)))))
            .count();
        assert!(read > 0, "{key}: no read saw a put");
        assert_eq!(
            linear::check(&Register, ops, CHECK_BUDGET),
            Verdict::Linearizable,
            "{key}: {} operations",
            ops.len()
        );
    }
}
