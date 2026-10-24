//! The gateway host (docs/design/node.md §4.2, §5): for each object exchange a client or gateway
//! asks of the node (`crate::object`), the node runs the bucket's coordinator
//! (`mantle_meta::coordinator`) or the gateway's drivers (`mantle_gateway::put`, `get`), and serves
//! each one's requests itself: a placement from its own replica of the volume table
//! (`crate::place`), a chunk on its own volume in-process or on a peer's over the transfer class,
//! and a range's command or read of the range's leader (`Node::command`, `Node::read`).
//!
//! A put's body is read from its exchange only as fast as the driver takes it, a piece at a time,
//! and a get's bytes are taken from its driver only as fast as the exchange takes them, so what an
//! operation holds is its driver's window of blocks and one piece. The operations the host runs at
//! once are bounded (`GatewaySettings::objects`); one past them is answered `SlowDown`.

use std::collections::VecDeque;

use bytes::Bytes;
use hyper_transport::{ExchangeId, PeerId, Refusal};
use mantle_ec::durability::{self, Choice, Rates};
use mantle_gateway::get::{self, Get, GetError, Keyring};
use mantle_gateway::layout::Layout;
use mantle_gateway::put::{self, Body, Commit, Expected, Holding, Keys, Put, PutError, Random};
use mantle_meta::apply::Layer;
use mantle_meta::coordinator::{self, Coordinator, Settled};
use mantle_meta::name::{self, Preconditions};
use mantle_meta::record::{BucketState, Descriptor, Version, WrappedKey, Wrapper};
use mantle_meta::root::{self, Placed};
use mantle_meta::wire::{Answer, Command};
use mantle_meta::{bucket, engine::Engine};
use mantle_s3::seal::{DataKey, WrappingKey};

use super::{ChunkAsk, ChunkOutcome, Node, NodeError, NodeLog, PIECE, take_done};
use crate::ask::{CommandAsk, CommandOutcome, ReadAsk, ReadOutcome};
use crate::classes::Class;
use crate::object::{Head, Op, Reply, Status};
use crate::place;
use crate::read::{Query, Read};

/// What the gateway host is given (docs/design/node.md §4.6, §5).
pub struct GatewaySettings {
    /// The root key's generations, the last the current one, which new files' data keys are
    /// wrapped under (encryption.md §2): a file is opened under the generation its header names.
    pub keys: Vec<(u32, WrappingKey)>,
    /// How long a block or file is held for the write above it (gateway.md §2).
    pub handover_ns: u64,
    /// The blocks a put holds going down, and a get holds being read, at once (gateway.md §2–§3).
    pub window: usize,
    /// The object operations the host runs at once; one past them is answered `SlowDown`.
    pub objects: usize,
    /// The buckets an owner may have (S3's 10,000, research/05 §10.4).
    pub bucket_quota: u32,
    /// The rows a bucket's coordinator passes in one read or collection (`coordinator::Bounds`).
    pub scan_rows: u32,
    /// The durability a block's scheme meets: the chunk loss and repair rates the model takes,
    /// and the annual loss probability allowed (durability.md; `mantle_ec::durability::choose`).
    pub rates: Rates,
    pub target: f64,
}

/// What the host waits on for an operation: a command, a read, or a chunk it asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Pending {
    Command(CommandAsk),
    Read(ReadAsk),
    Chunk(ChunkAsk),
}

/// An object operation the host runs: the exchange it answers, its asker, and where it is.
pub(crate) struct Job {
    exchange: ExchangeId,
    peer: PeerId,
    work: Work,
    /// What the operation waits on, each with the driver's request it answers, if a driver's.
    pending: Vec<(Pending, Option<u64>)>,
}

enum Work {
    /// The request's names being read from its body's start: the head, the names so far.
    Naming(Head, Vec<u8>),
    /// A request refused or done: what is left of its body read and dropped, then the reply, so
    /// the exchange ends whole (`Arriving::Redirect`'s reason).
    Draining(Reply),
    /// A reply's head sent: the tags that open its body, and how much of them is written.
    Replying(Vec<u8>, usize),
    Create(Create),
    Put(Box<Putting>),
    Get(Box<Getting>),
}

struct Create {
    bucket: String,
    coordinator: Option<Coordinator>,
}

struct Putting {
    bucket: String,
    key: String,
    id: u128,
    length: u64,
    /// The driver, once the bucket's row was read.
    put: Option<Put>,
    /// Body bytes read and not yet taken by the driver, and the body's bytes taken so far.
    carry: Vec<u8>,
    taken: u64,
    /// The driver's requests not yet asked: the node held as many asks as it keeps.
    waiting: VecDeque<(u64, put::Request)>,
}

struct Getting {
    bucket: String,
    key: String,
    offset: u64,
    len: u64,
    get: Option<Get>,
    /// Plaintext taken from the driver and not yet written to the exchange.
    out: Option<(Bytes, usize)>,
    /// Bytes of the range written so far, and the range's length.
    written: u64,
    total: u64,
    waiting: VecDeque<(u64, get::Request)>,
}

/// The volume table as this node's member of the root range held it after an entry it applied,
/// and the scheme new blocks take over it: each derived once per entry the member applies, the
/// only thing that changes them.
pub(crate) struct Table {
    applied: u64,
    /// In the order of their ids (`root::volumes`).
    placed: Vec<Placed>,
    scheme: Option<durability::Scheme>,
}

impl Table {
    /// The node that holds `volume`, if the table holds it.
    fn node_of(&self, volume: u128) -> Option<PeerId> {
        let at = self
            .placed
            .binary_search_by_key(&volume, |p| p.volume)
            .ok()?;
        self.placed.get(at).map(|p| p.node)
    }
}

/// The scheme a new block takes over `table`: the durability model's choice among the schemes that
/// fit the cell's failure domains, or the closest to the target where none meets it, as a cell
/// short of domains places on those there are (node.md §1.5).
fn scheme(gateway: &GatewaySettings, table: &[Placed]) -> Option<durability::Scheme> {
    let mut domains: Vec<&str> = table.iter().map(|p| p.domain.as_str()).collect();
    domains.sort_unstable();
    domains.dedup();
    match durability::choose(
        &durability::candidates(),
        domains.len(),
        &gateway.rates,
        gateway.target,
    ) {
        Ok(Some(Choice::Meets { scheme, .. } | Choice::Short { scheme, .. })) => Some(scheme),
        _ => None,
    }
}

/// The data keys of the files a get opens, unwrapped under the root key's generation each names.
struct RootKeys {
    keys: Vec<(u32, [u8; 32])>,
}

impl Keyring for RootKeys {
    fn unwrap(&self, key: &WrappedKey) -> Result<DataKey, GetError> {
        let Wrapper::Root(generation) = key.by else {
            return Err(GetError::Key);
        };
        let (_, bytes) = self
            .keys
            .iter()
            .find(|(g, _)| *g == generation)
            .ok_or(GetError::Key)?;
        DataKey::unwrap(&key.bytes, &WrappingKey::new(bytes)).map_err(|_| GetError::Key)
    }
}

/// The status a put's failure is answered with.
fn put_status(error: &PutError) -> Status {
    match error {
        PutError::IncompleteBody => Status::IncompleteBody,
        PutError::EntityTooLarge { .. } => Status::EntityTooLarge,
        PutError::Name(name::Outcome::NoSuchBucket) => Status::NoSuchBucket,
        _ => Status::Failed,
    }
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// An object's request arrived on `exchange` from `peer`, its body `body` bytes: a job that
    /// reads its names first, or the reply it gets at once.
    pub(super) fn take_object(&mut self, exchange: ExchangeId, peer: PeerId, body: Option<u64>) {
        let head = Head::decode(
            self.driver.endpoint().head(exchange).unwrap_or_default(),
            body,
        );
        let work = match (head, self.gateway.as_ref()) {
            (_, None) => Work::Draining(Reply::status(Status::Failed)),
            (Err(_), Some(_)) => Work::Draining(Reply::status(Status::Malformed)),
            (Ok(_), Some(g)) if self.jobs.len() >= g.objects => {
                Work::Draining(Reply::status(Status::SlowDown))
            }
            (Ok(head), Some(_)) => {
                let names = usize::try_from(head.names()).unwrap_or(0);
                Work::Naming(head, Vec::with_capacity(names))
            }
        };
        self.jobs.push(Job {
            exchange,
            peer,
            work,
            pending: Vec::new(),
        });
    }

    /// A job's names read from its body's start, a piece at a time; once whole, the work they
    /// name, the bytes read past them the start of a put's body.
    fn read_names(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let exchange = job.exchange;
        let Work::Naming(head, names) = &mut job.work else {
            return Ok(false);
        };
        let wanted = usize::try_from(head.names()).unwrap_or(usize::MAX);
        let endpoint = self.driver.endpoint();
        while names.len() < wanted {
            let Ok(mut piece) = endpoint.reserve(Class::Transfer, PIECE) else {
                return Ok(false);
            };
            let got = endpoint.read_body(exchange, &mut piece);
            names.extend_from_slice(piece.bytes());
            self.moved = self
                .moved
                .saturating_add(u64::try_from(piece.bytes().len()).unwrap_or(u64::MAX));
            endpoint.release(piece);
            match got {
                Ok(0) if names.len() < wanted => return Ok(false),
                Ok(_) => {}
                // Its asker is gone.
                Err(_) => return Ok(true),
            }
        }
        let carry = names.split_off(wanted.min(names.len()));
        let work = match head.op(names) {
            Err(_) => Work::Draining(Reply::status(Status::Malformed)),
            Ok(Op::Create { bucket }) => Work::Create(Create {
                bucket,
                coordinator: None,
            }),
            Ok(Op::Put { length, .. }) if length > mantle_s3::body::MAX_UPLOAD => {
                Work::Draining(Reply::status(Status::EntityTooLarge))
            }
            Ok(Op::Put {
                bucket,
                key,
                id,
                length,
            }) => Work::Put(Box::new(Putting {
                bucket,
                key,
                id,
                length,
                put: None,
                taken: 0,
                carry,
                waiting: VecDeque::new(),
            })),
            Ok(Op::Get {
                bucket,
                key,
                offset,
                len,
            }) => Work::Get(Box::new(Getting {
                bucket,
                key,
                offset,
                len,
                get: None,
                out: None,
                written: 0,
                total: 0,
                waiting: VecDeque::new(),
            })),
        };
        job.work = work;
        Ok(false)
    }

    /// The group of the range this node hosts of `layer`: the cell's one range of it.
    fn group_of(&self, layer: Layer) -> Option<u128> {
        self.ranges
            .iter()
            .find(|k| k.layer == layer)
            .map(|k| k.group)
    }

    /// The volume table as this node's member of the root range holds it now: read from the
    /// range's rows again only once the member applied an entry since the last read.
    fn table(&mut self) -> Result<&Table, NodeError> {
        let handle = self
            .ranges
            .iter()
            .find(|k| k.layer == Layer::Root)
            .map(|k| k.handle)
            .ok_or(NodeError::Bounds("no root range here".into()))?;
        let shell = self
            .owner
            .get_mut(handle)
            .ok_or(NodeError::Bounds("no root range here".into()))?;
        let applied = shell.applied().index;
        if self.table.as_ref().is_none_or(|t| t.applied != applied) {
            let engine = shell.machine_mut().engine_mut();
            let census = root::census(engine).map_err(|e| NodeError::Bounds(e.to_string()))?;
            let most = usize::try_from(census.volumes).unwrap_or(usize::MAX);
            let placed =
                root::volumes(engine, None, most).map_err(|e| NodeError::Bounds(e.to_string()))?;
            let scheme = self.gateway.as_ref().and_then(|g| scheme(g, &placed));
            self.table = Some(Table {
                applied,
                placed,
                scheme,
            });
        }
        self.table
            .as_ref()
            .ok_or(NodeError::Bounds("no volume table".into()))
    }

    /// The host at a turn: each operation's answers taken, its next requests asked, its body read
    /// and its bytes written as far as they go, and each one done answered.
    pub(super) fn step_host(&mut self) -> Result<(), NodeError> {
        self.drop_abandoned();
        if self.jobs.is_empty() {
            return Ok(());
        }
        // Every operation's answers taken before any asks again, so each asks with the room
        // that every answer taken this turn gave back.
        for at in 0..self.jobs.len() {
            self.take_answers(at)?;
        }
        let mut at = 0;
        while at < self.jobs.len() {
            if self.step_job(at)? {
                // What it still waits on is let go: an ended get's chunks still out, say.
                let job = self.jobs.swap_remove(at);
                self.abandoned
                    .extend(job.pending.into_iter().map(|(pending, _)| pending));
            } else {
                at = at.saturating_add(1);
            }
        }
        Ok(())
    }

    /// One operation's turn; whether it is over.
    fn step_job(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get(at) else {
            return Ok(true);
        };
        match job.work {
            Work::Naming(..) => self.read_names(at),
            Work::Draining(_) => self.drain_job(at),
            Work::Replying(..) => self.write_tags(at),
            Work::Create(_) => self.step_create(at),
            Work::Put(_) => self.step_put(at),
            Work::Get(_) => self.step_get(at),
        }
    }

    /// The outcomes of what job `at` asked, taken from the node's answers and given to it.
    fn take_answers(&mut self, at: usize) -> Result<(), NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(());
        };
        if job.pending.is_empty() {
            return Ok(());
        }
        let mut answered: Vec<(Option<u64>, Outcome)> = Vec::new();
        job.pending.retain(|(pending, request)| {
            let outcome = match pending {
                Pending::Command(ask) => {
                    take_done(&mut self.commands_done, *ask).map(Outcome::Command)
                }
                Pending::Read(ask) => take_done(&mut self.reads_done, *ask).map(Outcome::Read),
                Pending::Chunk(ask) => take_done(&mut self.chunks_done, *ask).map(Outcome::Chunk),
            };
            match outcome {
                Some(outcome) => {
                    answered.push((*request, outcome));
                    false
                }
                None => true,
            }
        });
        for (request, outcome) in answered {
            self.answer_job(at, request, outcome)?;
        }
        Ok(())
    }

    /// Gives job `at` an outcome of what it asked.
    fn answer_job(
        &mut self,
        at: usize,
        request: Option<u64>,
        outcome: Outcome,
    ) -> Result<(), NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(());
        };
        match &mut job.work {
            Work::Naming(..) | Work::Draining(_) | Work::Replying(..) => {}
            Work::Create(create) => {
                let answer = match outcome {
                    Outcome::Command(CommandOutcome::Answered(Answer::Bucket(o))) => {
                        if create.coordinator.is_none() {
                            return self.created(at, o);
                        }
                        coordinator::Answer::Bucket(o)
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => {
                        coordinator::Answer::Name(o)
                    }
                    Outcome::Read(ReadOutcome::Read(read)) => match *read {
                        Read::Gate(gate) => coordinator::Answer::Gate(gate),
                        _ => return self.fail_job(at, Status::Failed),
                    },
                    _ => return self.fail_job(at, Status::Failed),
                };
                let Some(c) = create.coordinator.as_mut() else {
                    return self.fail_job(at, Status::Failed);
                };
                if c.answer(answer).is_err() {
                    return self.fail_job(at, Status::Failed);
                }
            }
            Work::Put(putting) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.bucket_read(at, *read);
                }
                (None, _) => return self.fail_job(at, Status::Failed),
                (Some(id), outcome) => {
                    let answer = put_answer(outcome);
                    if let Some(put) = putting.put.as_mut()
                        && put.answer(id, answer).is_err()
                    {
                        return self.fail_job(at, Status::Failed);
                    }
                }
            },
            Work::Get(getting) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.current_read(at, *read);
                }
                (None, _) => return self.fail_job(at, Status::Failed),
                (Some(id), outcome) => {
                    let answer = get_answer(outcome);
                    if let Some(get) = getting.get.as_mut()
                        && get.answer(id, answer).is_err()
                    {
                        return self.fail_job(at, Status::Failed);
                    }
                }
            },
        }
        Ok(())
    }

    /// A create's Bucket-range answer to its `Create`: its coordinator, or the reply it gets.
    fn created(&mut self, at: usize, outcome: bucket::Outcome) -> Result<(), NodeError> {
        let status = match outcome {
            bucket::Outcome::Creating { .. } => None,
            bucket::Outcome::AlreadyOwnedByYou => Some(Status::BucketAlreadyOwnedByYou),
            bucket::Outcome::AlreadyExists => Some(Status::BucketAlreadyExists),
            bucket::Outcome::TooManyBuckets => Some(Status::TooManyBuckets),
            bucket::Outcome::OperationAborted => Some(Status::OperationAborted),
            _ => Some(Status::Failed),
        };
        if let Some(status) = status {
            return self.fail_job(at, status);
        }
        let Some(gateway) = self.gateway.as_ref() else {
            return self.fail_job(at, Status::Failed);
        };
        let bounds = coordinator::Bounds {
            budget: gateway.scan_rows,
            ranges: self.ranges.len(),
        };
        // The cell's first Name range spans every key at its first generation (node.md §1.5).
        let directory = [Descriptor {
            id: 1,
            lo: Vec::new(),
            hi: None,
            generation: 1,
        }];
        let Some(Job {
            work: Work::Create(create),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        match Coordinator::start(&create.bucket, &outcome, &directory, bounds) {
            Ok(Some(c)) => {
                create.coordinator = Some(c);
                Ok(())
            }
            _ => self.fail_job(at, Status::Failed),
        }
    }

    /// A create's turn: its `Create` asked first, then its coordinator's next request, one at a
    /// time; once the coordinator is done, its answer.
    fn step_create(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get(at) else {
            return Ok(true);
        };
        if !job.pending.is_empty() {
            return Ok(false);
        }
        let Work::Create(create) = &job.work else {
            return Ok(true);
        };
        let (Some(bucket_group), Some(name_group)) =
            (self.group_of(Layer::Bucket), self.group_of(Layer::Name))
        else {
            self.fail_job(at, Status::Failed)?;
            return self.drain_job(at);
        };
        let owner = format!("{}", job.peer);
        let request = match &create.coordinator {
            None => {
                let quota = self.gateway.as_ref().map_or(0, |g| g.bucket_quota);
                Some(coordinator::Request::Bucket(bucket::Command::Create(
                    bucket::Create {
                        bucket: create.bucket.clone(),
                        owner,
                        location: String::new(),
                        at_ns: 0,
                        quota,
                        lock: false,
                    },
                )))
            }
            Some(c) if c.is_done() => {
                let status = match c.settled() {
                    Some(Settled::Created) => Status::Done,
                    _ => Status::OperationAborted,
                };
                self.fail_job(at, status)?;
                return self.drain_job(at);
            }
            Some(c) => c.next(),
        };
        let bucket = create.bucket.clone();
        let asked = match request {
            Some(coordinator::Request::Bucket(command)) => self
                .command(bucket_group, Command::Bucket(command), u64::MAX)
                .map(Pending::Command),
            Some(coordinator::Request::Name { command, .. }) => self
                .command(name_group, Command::Name(command), u64::MAX)
                .map(Pending::Command),
            Some(coordinator::Request::Gate { generation, .. }) => self
                .read(name_group, Query::Gate { bucket, generation }, u64::MAX)
                .map(Pending::Read),
            // The cell's one Name range is the directory, and a create probes nothing.
            Some(coordinator::Request::Directory) => {
                let directory = vec![Descriptor {
                    id: 1,
                    lo: Vec::new(),
                    hi: None,
                    generation: 1,
                }];
                if let Some(Job {
                    work:
                        Work::Create(Create {
                            coordinator: Some(c),
                            ..
                        }),
                    ..
                }) = self.jobs.get_mut(at)
                    && c.answer(coordinator::Answer::Directory(directory)).is_err()
                {
                    self.fail_job(at, Status::Failed)?;
                }
                return Ok(false);
            }
            Some(coordinator::Request::Probe { .. }) | None => {
                self.fail_job(at, Status::Failed)?;
                return self.drain_job(at);
            }
        };
        match asked {
            Ok(pending) => {
                if let Some(job) = self.jobs.get_mut(at) {
                    job.pending.push((pending, None));
                }
            }
            // The node holds as many asks as it keeps: asked again at the next turn.
            Err(NodeError::Asks) => {}
            Err(_) => {
                self.fail_job(at, Status::Failed)?;
                return self.drain_job(at);
            }
        }
        Ok(false)
    }

    /// A put's bucket row, read: its driver made over it, or the reply `NoSuchBucket`.
    fn bucket_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let Read::Bucket(Some(row)) = read else {
            return self.fail_job(at, Status::NoSuchBucket);
        };
        if row.state != BucketState::Active {
            return self.fail_job(at, Status::NoSuchBucket);
        }
        let Ok(Some(scheme)) = self.table().map(|t| t.scheme) else {
            return self.fail_job(at, Status::Failed);
        };
        let now = self.wall.now_ns();
        let Some(gateway) = self.gateway.as_ref() else {
            return self.fail_job(at, Status::Failed);
        };
        let Some((generation, root)) = gateway.keys.last() else {
            return self.fail_job(at, Status::Failed);
        };
        let holding = Holding {
            handover_ns: gateway.handover_ns,
            window: gateway.window,
        };
        let made = (|| {
            let data = DataKey::generate().ok()?;
            let wrapped = WrappedKey {
                by: Wrapper::Root(*generation),
                bytes: data.wrap(root).ok()?,
            };
            let mut file = [0u8; 16];
            getrandom::fill(&mut file).ok()?;
            let keys = Keys {
                file: u128::from_be_bytes(file),
                data,
                wrapped,
            };
            Some((keys, Layout::new(scheme).ok()?))
        })();
        let Some((keys, layout)) = made else {
            return self.fail_job(at, Status::Failed);
        };
        let default = row.lock.and_then(|lock| lock.default);
        let Some(Job {
            work: Work::Put(putting),
            peer,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let commit = Commit::Object(name::Put {
            bucket: putting.bucket.clone(),
            incarnation: row.created_ns,
            key: putting.key.clone(),
            versioning: row.versioning,
            preconditions: Preconditions::default(),
            at_ns: 0,
            ordered_ns: None,
            version: Version {
                marker: false,
                null: false,
                modified_ns: 0,
                etag: String::new(),
                size: 0,
                checksum: None,
                file: None,
                owner: format!("{peer}"),
                headers: Vec::new(),
                retention: None,
                legal_hold: None,
                listing: None,
            },
            default,
            id: putting.id,
            deadline_ns: 0,
        });
        let body = Body {
            length: putting.length,
            checksum: None,
            content_md5: false,
        };
        match Put::new(commit, body, keys, layout, holding, Box::new(Random), now) {
            Ok(put) => {
                putting.put = Some(put);
                Ok(())
            }
            Err(error) => {
                {
                    use std::io::Write as _;
                    let _ = writeln!(std::io::stderr(), "DIAG put new failed: {error:?}");
                }
                let status = put_status(&error);
                self.fail_job(at, status)
            }
        }
    }

    /// A put's turn: its bucket's row read first; then its body read as far as the driver takes
    /// it, its driver's time moved and its requests asked; once its outcome is known, its answer.
    fn step_put(&mut self, at: usize) -> Result<bool, NodeError> {
        let wall = self.wall.now_ns();
        let state = {
            let Some(Job {
                work: Work::Put(putting),
                pending,
                ..
            }) = self.jobs.get_mut(at)
            else {
                return Ok(true);
            };
            match putting.put.as_mut() {
                None if pending.is_empty() => Step::Ask(putting.bucket.clone()),
                None => Step::Wait,
                Some(put) => {
                    let over = put.outcome().map(|outcome| match outcome {
                        Ok(stored) => Reply {
                            status: Status::Done,
                            version: stored.version.clone().unwrap_or_default(),
                            etag: stored.etag.clone(),
                            size: stored.size,
                        },
                        Err(error) => {
                            use std::io::Write as _;
                            let _ = writeln!(std::io::stderr(), "DIAG put failed: {error:?}");
                            Reply::status(put_status(error))
                        }
                    });
                    match over {
                        Some(reply) => Step::Over(reply),
                        None if put.tick(wall).is_err() => {
                            use std::io::Write as _;
                            let _ = writeln!(std::io::stderr(), "DIAG put tick failed");
                            Step::Over(Reply::status(Status::Failed))
                        }
                        None => Step::Go,
                    }
                }
            }
        };
        match state {
            Step::Wait => return Ok(false),
            Step::Over(reply) => {
                if let Some(job) = self.jobs.get_mut(at) {
                    job.work = Work::Draining(reply);
                }
                return self.drain_job(at);
            }
            Step::Ask(bucket) => {
                let Some(group) = self.group_of(Layer::Bucket) else {
                    self.fail_job(at, Status::Failed)?;
                    return self.drain_job(at);
                };
                match self.read(group, Query::Bucket { bucket }, u64::MAX) {
                    Ok(ask) => {
                        if let Some(job) = self.jobs.get_mut(at) {
                            job.pending.push((Pending::Read(ask), None));
                        }
                    }
                    Err(NodeError::Asks) => {}
                    Err(_) => {
                        self.fail_job(at, Status::Failed)?;
                        return self.drain_job(at);
                    }
                }
                return Ok(false);
            }
            Step::Go => {}
        }
        self.feed_put(at)?;
        // The driver's requests, each asked of what serves it.
        loop {
            let Some(Job {
                work: Work::Put(putting),
                ..
            }) = self.jobs.get_mut(at)
            else {
                return Ok(true);
            };
            let next = match putting.waiting.pop_front() {
                Some(request) => Some(request),
                None => putting.put.as_mut().and_then(Put::poll),
            };
            let Some((id, request)) = next else {
                break;
            };
            // Not asked: the node holds as many asks as it keeps. It waits, in its place.
            if let Some(request) = self.ask_put(at, id, request)? {
                if let Some(Job {
                    work: Work::Put(putting),
                    ..
                }) = self.jobs.get_mut(at)
                {
                    putting.waiting.push_front((id, request));
                }
                break;
            }
        }
        Ok(false)
    }

    /// Reads a put's body into its driver as far as the driver takes it: what it left of the
    /// last piece first, then a piece at a time; at the body's end, the body ended.
    fn feed_put(&mut self, at: usize) -> Result<(), NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(());
        };
        let exchange = job.exchange;
        let Work::Put(putting) = &mut job.work else {
            return Ok(());
        };
        let Some(put) = putting.put.as_mut() else {
            return Ok(());
        };
        let endpoint = self.driver.endpoint();
        loop {
            if !putting.carry.is_empty() {
                if !put.wants_body() {
                    return Ok(());
                }
                let took = match put.feed(&putting.carry) {
                    Ok(took) => took,
                    Err(_) => return self.fail_job(at, Status::Failed),
                };
                putting.taken = putting
                    .taken
                    .saturating_add(u64::try_from(took).unwrap_or(u64::MAX));
                putting.carry.drain(..took.min(putting.carry.len()));
                if !putting.carry.is_empty() {
                    return Ok(());
                }
            }
            if putting.taken >= putting.length {
                if endpoint.body_complete(exchange) || putting.length == 0 {
                    if put.end(&Expected::default()).is_err() && put.outcome().is_none() {
                        return self.fail_job(at, Status::Failed);
                    }
                    // Ended once: nothing more is read, and the outcome answers it.
                    putting.length = u64::MAX;
                } else {
                    // The body's checksum, after its last byte.
                    let mut empty = hyper_transport::Reservation::default();
                    let _ = endpoint.read_body(exchange, &mut empty);
                }
                return Ok(());
            }
            if !put.wants_body() {
                return Ok(());
            }
            let Ok(mut piece) = endpoint.reserve(Class::Transfer, PIECE) else {
                return Ok(());
            };
            let got = endpoint.read_body(exchange, &mut piece);
            putting.carry.extend_from_slice(piece.bytes());
            self.moved = self
                .moved
                .saturating_add(u64::try_from(piece.bytes().len()).unwrap_or(u64::MAX));
            endpoint.release(piece);
            match got {
                Ok(0) if putting.carry.is_empty() => return Ok(()),
                Ok(_) => {}
                Err(_) => return self.fail_job(at, Status::IncompleteBody),
            }
        }
    }

    /// Asks a put's driver's request of what serves it; the request back when the node holds as
    /// many asks as it keeps.
    fn ask_put(
        &mut self,
        at: usize,
        id: u64,
        request: put::Request,
    ) -> Result<Option<put::Request>, NodeError> {
        let asked = match &request {
            put::Request::Place { block, .. } => {
                let Ok(volumes) = self.table().map(|t| place::order(*block, &t.placed)) else {
                    self.fail_job(at, Status::Failed)?;
                    return Ok(None);
                };
                if let Some(Job {
                    work: Work::Put(putting),
                    ..
                }) = self.jobs.get_mut(at)
                    && let Some(put) = putting.put.as_mut()
                    && put.answer(id, put::Answer::Volumes(volumes)).is_err()
                {
                    self.fail_job(at, Status::Failed)?;
                }
                return Ok(None);
            }
            put::Request::Chunk {
                volume,
                key,
                bytes,
                crc,
            } => {
                let Ok(node) = self.table().map(|t| t.node_of(*volume)) else {
                    self.fail_job(at, Status::Failed)?;
                    return Ok(None);
                };
                let Some(node) = node else {
                    // A volume the table no longer holds refuses it: the next is asked.
                    if let Some(Job {
                        work: Work::Put(putting),
                        ..
                    }) = self.jobs.get_mut(at)
                        && let Some(put) = putting.put.as_mut()
                        && put.answer(id, put::Answer::Refused).is_err()
                    {
                        self.fail_job(at, Status::Failed)?;
                    }
                    return Ok(None);
                };
                self.put_chunk(node, *volume, *key, bytes.to_vec(), *crc)
                    .map(Pending::Chunk)
            }
            put::Request::Block(command) => self
                .group_of(Layer::Block)
                .ok_or(NodeError::Bounds("no Block range here".into()))
                .and_then(|g| self.command(g, Command::Block(command.clone()), u64::MAX))
                .map(Pending::Command),
            put::Request::File(command) => self
                .group_of(Layer::File)
                .ok_or(NodeError::Bounds("no File range here".into()))
                .and_then(|g| self.command(g, Command::File(command.clone()), u64::MAX))
                .map(Pending::Command),
            put::Request::Name(command) => self
                .group_of(Layer::Name)
                .ok_or(NodeError::Bounds("no Name range here".into()))
                .and_then(|g| self.command(g, Command::Name(command.clone()), u64::MAX))
                .map(Pending::Command),
        };
        match asked {
            Ok(pending) => {
                if let Some(job) = self.jobs.get_mut(at) {
                    job.pending.push((pending, Some(id)));
                }
                Ok(None)
            }
            Err(NodeError::Asks) => Ok(Some(request)),
            Err(_) => {
                self.fail_job(at, Status::Failed)?;
                Ok(None)
            }
        }
    }

    /// A get's current version, read: its driver made over it and the reply's head sent, or the
    /// reply `NoSuchKey` or `InvalidRange`.
    fn current_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let Read::Current(Some((order, version))) = read else {
            return self.fail_job(at, Status::NoSuchKey);
        };
        if version.marker {
            return self.fail_job(at, Status::NoSuchKey);
        }
        let Some(gateway) = self.gateway.as_ref() else {
            return self.fail_job(at, Status::Failed);
        };
        let keyring = RootKeys {
            keys: gateway
                .keys
                .iter()
                .map(|(g, key)| (*g, *key.bytes()))
                .collect(),
        };
        let window = gateway.window;
        let Some(Job {
            work: Work::Get(getting),
            exchange,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let exchange = *exchange;
        let size = version.size;
        if getting.offset > size || (getting.offset == size && size > 0) {
            return self.fail_job(at, Status::InvalidRange);
        }
        let end = getting.offset.saturating_add(getting.len).min(size);
        let total = end.saturating_sub(getting.offset);
        match Get::new(
            version.file,
            size,
            getting.offset..end,
            Box::new(keyring),
            window,
        ) {
            Ok(get) => {
                getting.get = Some(get);
                getting.total = total;
            }
            Err(_) => return self.fail_job(at, Status::Failed),
        }
        let reply = Reply {
            status: Status::Done,
            version: mantle_meta::key::version_id(order),
            etag: version.etag,
            size,
        };
        let Ok((head, tags)) = reply.encode() else {
            return self.fail_job(at, Status::Failed);
        };
        // The tags open the body, ahead of the range's bytes.
        let tagged = u64::try_from(tags.len()).unwrap_or(u64::MAX);
        if let Some(Job {
            work: Work::Get(getting),
            ..
        }) = self.jobs.get_mut(at)
        {
            getting.total = total.saturating_add(tagged);
            getting.out = Some((Bytes::from(tags), 0));
        }
        match self
            .driver
            .endpoint()
            .reply(exchange, &head, Some(total.saturating_add(tagged)))
        {
            Ok(()) => Ok(()),
            // Its asker is gone: the job ends with its exchange.
            Err(_) => self.fail_job(at, Status::Failed),
        }
    }

    /// A get's turn: its current version read first; then its driver's requests asked and its
    /// bytes written as far as the exchange takes them; once every byte is written, its end.
    fn step_get(&mut self, at: usize) -> Result<bool, NodeError> {
        let name = self.group_of(Layer::Name);
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let Work::Get(getting) = &mut job.work else {
            return Ok(true);
        };
        if getting.get.is_none() {
            if job.pending.is_empty() {
                let query = Query::Current {
                    bucket: getting.bucket.clone(),
                    key: getting.key.clone(),
                };
                let Some(group) = name else {
                    self.fail_job(at, Status::Failed)?;
                    return self.drain_job(at);
                };
                match self.read(group, query, u64::MAX) {
                    Ok(ask) => {
                        if let Some(job) = self.jobs.get_mut(at) {
                            job.pending.push((Pending::Read(ask), None));
                        }
                    }
                    Err(NodeError::Asks) => {}
                    Err(_) => {
                        self.fail_job(at, Status::Failed)?;
                        return self.drain_job(at);
                    }
                }
            }
            return Ok(false);
        }
        if self.write_get(at)? {
            return Ok(true);
        }
        loop {
            let Some(Job {
                work: Work::Get(getting),
                ..
            }) = self.jobs.get_mut(at)
            else {
                return Ok(true);
            };
            let next = match getting.waiting.pop_front() {
                Some(request) => Some(request),
                None => getting.get.as_mut().and_then(Get::poll),
            };
            let Some((id, request)) = next else {
                break;
            };
            if let Some(request) = self.ask_get(at, id, request)? {
                if let Some(Job {
                    work: Work::Get(getting),
                    ..
                }) = self.jobs.get_mut(at)
                {
                    getting.waiting.push_front((id, request));
                }
                break;
            }
        }
        Ok(false)
    }

    /// Writes a get's bytes as far as its exchange takes them, taking more from the driver as the
    /// last is written; whether the get is over: every byte written, or its driver or exchange
    /// failed, which ends the exchange unfinished.
    fn write_get(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let exchange = job.exchange;
        let Work::Get(getting) = &mut job.work else {
            return Ok(true);
        };
        let Some(get) = getting.get.as_mut() else {
            return Ok(false);
        };
        let endpoint = self.driver.endpoint();
        loop {
            if getting.out.is_none() {
                getting.out = get.take().map(|bytes| (bytes, 0));
            }
            let Some((bytes, written)) = getting.out.as_mut() else {
                break;
            };
            let Some(rest) = bytes.get(*written..) else {
                getting.out = None;
                continue;
            };
            if rest.is_empty() {
                getting.out = None;
                continue;
            }
            match endpoint.write_body(exchange, rest) {
                Ok(0) => return Ok(false),
                Ok(took) => {
                    let took_bytes = u64::try_from(took).unwrap_or(u64::MAX);
                    *written = written.saturating_add(took);
                    getting.written = getting.written.saturating_add(took_bytes);
                    self.moved = self.moved.saturating_add(took_bytes);
                }
                Err(_) => {
                    endpoint.refuse_exchange(exchange, Refusal::Closed);
                    return Ok(true);
                }
            }
        }
        match get.outcome() {
            Some(Ok(())) if getting.written >= getting.total => {
                endpoint.end(exchange);
                Ok(true)
            }
            // The driver failed past the reply's head: the exchange ends unfinished, which its
            // asker hears as the exchange refused, not as bytes it could take for the object's.
            Some(Err(_)) => {
                endpoint.refuse_exchange(exchange, Refusal::Closed);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Asks a get's driver's request of what serves it; the request back when the node holds as
    /// many asks as it keeps.
    fn ask_get(
        &mut self,
        at: usize,
        id: u64,
        request: get::Request,
    ) -> Result<Option<get::Request>, NodeError> {
        let asked = match &request {
            get::Request::Header { file } => self
                .group_of(Layer::File)
                .ok_or(NodeError::Bounds("no File range here".into()))
                .and_then(|g| self.read(g, Query::Header { file: *file }, u64::MAX))
                .map(Pending::Read),
            get::Request::Extents { file, offset, max } => {
                let max = u32::try_from(*max).unwrap_or(u32::MAX);
                self.group_of(Layer::File)
                    .ok_or(NodeError::Bounds("no File range here".into()))
                    .and_then(|g| {
                        let query = Query::Extents {
                            file: *file,
                            offset: *offset,
                            max,
                        };
                        self.read(g, query, u64::MAX)
                    })
                    .map(Pending::Read)
            }
            get::Request::Block { block } => self
                .group_of(Layer::Block)
                .ok_or(NodeError::Bounds("no Block range here".into()))
                .and_then(|g| self.read(g, Query::Block { block: *block }, u64::MAX))
                .map(Pending::Read),
            get::Request::Chunk {
                volume,
                key,
                offset,
                len,
            } => {
                let Ok(node) = self.table().map(|t| t.node_of(*volume)) else {
                    self.fail_job(at, Status::Failed)?;
                    return Ok(None);
                };
                let Some(node) = node else {
                    if let Some(Job {
                        work: Work::Get(getting),
                        ..
                    }) = self.jobs.get_mut(at)
                        && let Some(get) = getting.get.as_mut()
                        && get.answer(id, get::Answer::Unreadable).is_err()
                    {
                        self.fail_job(at, Status::Failed)?;
                    }
                    return Ok(None);
                };
                self.get_chunk(node, *volume, *key, *offset, *len)
                    .map(Pending::Chunk)
            }
        };
        match asked {
            Ok(pending) => {
                if let Some(job) = self.jobs.get_mut(at) {
                    job.pending.push((pending, Some(id)));
                }
                Ok(None)
            }
            Err(NodeError::Asks) => Ok(Some(request)),
            Err(_) => {
                self.fail_job(at, Status::Failed)?;
                Ok(None)
            }
        }
    }

    /// Ends job `at` with `status`: what it waits on is let go, and its body, if any is left, read
    /// to its end and dropped before the reply goes.
    fn fail_job(&mut self, at: usize, status: Status) -> Result<(), NodeError> {
        if let Some(job) = self.jobs.get_mut(at) {
            job.work = Work::Draining(Reply::status(status));
            self.abandoned
                .extend(job.pending.drain(..).map(|(pending, _)| pending));
        }
        Ok(())
    }

    /// The outcomes of asks whose job ended before they came, dropped as they come: an outcome no
    /// job takes would hold one of the node's asks for good.
    fn drop_abandoned(&mut self) {
        if self.abandoned.is_empty() {
            return;
        }
        let Self {
            abandoned,
            commands_done,
            reads_done,
            chunks_done,
            ..
        } = self;
        abandoned.retain(|pending| {
            let gone = match pending {
                Pending::Command(ask) => take_done(commands_done, *ask).is_some(),
                Pending::Read(ask) => take_done(reads_done, *ask).is_some(),
                Pending::Chunk(ask) => take_done(chunks_done, *ask).is_some(),
            };
            !gone
        });
    }

    /// A draining job's turn: its body read to its end and dropped, then its reply; whether it is
    /// over.
    fn drain_job(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get(at) else {
            return Ok(true);
        };
        let Work::Draining(reply) = &job.work else {
            return Ok(false);
        };
        let exchange = job.exchange;
        let endpoint = self.driver.endpoint();
        loop {
            let Ok(mut piece) = endpoint.reserve(Class::Transfer, PIECE) else {
                return Ok(false);
            };
            let got = endpoint.read_body(exchange, &mut piece);
            self.moved = self
                .moved
                .saturating_add(u64::try_from(piece.bytes().len()).unwrap_or(u64::MAX));
            endpoint.release(piece);
            match got {
                Ok(0) => break,
                Ok(_) => {}
                // Its asker is gone: nothing to answer.
                Err(_) => return Ok(true),
            }
        }
        if !endpoint.body_complete(exchange) && endpoint.head(exchange).is_some() {
            let mut empty = hyper_transport::Reservation::default();
            let _ = endpoint.read_body(exchange, &mut empty);
            if !endpoint.body_complete(exchange) {
                return Ok(false);
            }
        }
        let Ok((head, tags)) = reply.encode() else {
            return Ok(true);
        };
        let tagged = u64::try_from(tags.len()).unwrap_or(u64::MAX);
        if endpoint.reply(exchange, &head, Some(tagged)).is_err() {
            return Ok(true);
        }
        if let Some(job) = self.jobs.get_mut(at) {
            job.work = Work::Replying(tags, 0);
        }
        self.write_tags(at)
    }

    /// A reply's tags written as far as its exchange takes them; once all are, the exchange ends.
    fn write_tags(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let exchange = job.exchange;
        let Work::Replying(tags, written) = &mut job.work else {
            return Ok(false);
        };
        let endpoint = self.driver.endpoint();
        while let Some(rest) = tags.get(*written..)
            && !rest.is_empty()
        {
            match endpoint.write_body(exchange, rest) {
                Ok(0) => return Ok(false),
                Ok(took) => {
                    *written = written.saturating_add(took);
                    self.moved = self
                        .moved
                        .saturating_add(u64::try_from(took).unwrap_or(u64::MAX));
                }
                Err(_) => return Ok(true),
            }
        }
        endpoint.end(exchange);
        Ok(true)
    }

    /// The jobs whose exchange is `exchange`, for an event on it.
    pub(super) fn job_on(&self, exchange: ExchangeId) -> Option<usize> {
        self.jobs.iter().position(|j| j.exchange == exchange)
    }

    /// An exchange of a job ended unanswered: the job ends, what it waits on let go.
    pub(super) fn job_lost(&mut self, exchange: ExchangeId) {
        let Self {
            jobs, abandoned, ..
        } = self;
        jobs.retain_mut(|j| {
            if j.exchange != exchange {
                return true;
            }
            abandoned.extend(j.pending.drain(..).map(|(pending, _)| pending));
            false
        });
    }

    /// When the host next needs a turn: at once when what an operation, or one ended, asked is
    /// answered, as one answered after the host's step in a turn (a read this node's own member
    /// answers) waits on no event; else a put's renewal coming due.
    pub(super) fn host_due(&self) -> Option<u64> {
        let shard = hyper_rt::futures::now_ns();
        let answered = |pending: &Pending| match pending {
            Pending::Command(ask) => self.commands_done.iter().any(|(a, _)| a == ask),
            Pending::Read(ask) => self.reads_done.iter().any(|(a, _)| a == ask),
            Pending::Chunk(ask) => self.chunks_done.iter().any(|(a, _)| a == ask),
        };
        let mut pending = self
            .jobs
            .iter()
            .flat_map(|j| j.pending.iter().map(|(p, _)| p))
            .chain(&self.abandoned);
        if pending.any(answered) {
            return Some(shard);
        }
        let wall = self.wall.now_ns();
        self.jobs
            .iter()
            .filter_map(|j| match &j.work {
                Work::Put(putting) => putting.put.as_ref()?.due_ns(),
                _ => None,
            })
            .min()
            .map(|due| shard.saturating_add(due.saturating_sub(wall)))
    }
}

/// Where a put is at a turn.
enum Step {
    /// Its bucket's row to ask for.
    Ask(String),
    /// Waiting on what it asked.
    Wait,
    /// Going: its body and requests to move.
    Go,
    /// Over, with its reply.
    Over(Reply),
}

/// What a job's ask came to.
enum Outcome {
    Command(CommandOutcome),
    Read(ReadOutcome),
    Chunk(ChunkOutcome),
}

/// A put's driver's answer for what its request came to.
fn put_answer(outcome: Outcome) -> put::Answer {
    match outcome {
        Outcome::Chunk(ChunkOutcome::Stored) => put::Answer::Stored,
        Outcome::Chunk(_) => put::Answer::Refused,
        Outcome::Command(CommandOutcome::Answered(Answer::Block(o))) => put::Answer::Block(o),
        Outcome::Command(CommandOutcome::Answered(Answer::File(o))) => put::Answer::File(o),
        Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => put::Answer::Name(o),
        // Any other: the driver takes it as the wrong kind and fails the put.
        _ => put::Answer::Refused,
    }
}

/// A get's driver's answer for what its request came to.
fn get_answer(outcome: Outcome) -> get::Answer {
    match outcome {
        Outcome::Chunk(ChunkOutcome::Read(bytes)) => get::Answer::Chunk(Bytes::from(bytes)),
        Outcome::Chunk(_) => get::Answer::Unreadable,
        Outcome::Read(ReadOutcome::Read(read)) => match *read {
            Read::Header(header) => get::Answer::Header(header),
            Read::Extents(extents) => get::Answer::Extents(extents),
            Read::Block(block) => get::Answer::Block(block),
            _ => get::Answer::Unreadable,
        },
        _ => get::Answer::Unreadable,
    }
}
