//! The gateway host (docs/design/node.md §4.2, §5): for each object exchange a client or gateway
//! asks of the node (`mantle_protocol::object`), the node runs the bucket's coordinator
//! (`mantle_meta::coordinator`), the gateway's drivers (`mantle_gateway::put`, `get`) or the
//! ranges' commands and reads an operation needs, and serves each one's requests itself: a
//! placement from its own replica of the volume table (`crate::place`), a chunk on its own volume
//! in-process or on a peer's over the transfer class, and a range's command or read of the range's
//! leader (`Node::command`, `Node::read`).
//!
//! An operation is typed (`object::Op`) and answers with a typed reply (`object::Reply`): the
//! native protocol's encoding is taken where a request's fields are read and given where its
//! reply's head and fields are written, so another door hands the host the same operation. Its
//! preconditions and range are judged by `mantle_s3::conditional` and `mantle_s3::range`, a
//! write's at the Name range's commit (05 §2).
//!
//! A put's body is read from its exchange only as fast as the driver takes it, a piece at a time,
//! and a get's bytes are taken from its driver only as fast as the exchange takes them, so what an
//! operation holds is its driver's window of blocks and one piece. The operations the host runs at
//! once are bounded (`GatewaySettings::objects`); one past them is answered `SlowDown`.

use std::collections::VecDeque;

use bytes::Bytes;
use hyper_rt::sync::{ChannelReceiver, Sender, SyncError};
use hyper_transport::{ExchangeId, PeerId, Refusal};
use mantle_ec::durability::{self, Choice, Rates};
use mantle_gateway::complete::{self, CompleteError, Completion};
use mantle_gateway::get::{self, Get, GetError, Keyring};
use mantle_gateway::layout::Layout;
use mantle_gateway::put::{self, Body, Commit, Expected, Holding, Keys, Put, PutError, Random};
use mantle_meta::apply::Layer;
use mantle_meta::coordinator::{self, Coordinator, Settled};
use mantle_meta::name::{self, Match, Preconditions};
use mantle_meta::record::{
    self, BucketState, Descriptor, LISTING, Part, RunMd5, Runs, Upload, Version, Versioning,
    WrappedKey, Wrapper,
};
use mantle_meta::root::{self, Placed};
use mantle_meta::wire::{Answer, Command};
use mantle_meta::{bucket, engine::Engine};
use mantle_s3::checksum::{self, Algorithm};
use mantle_s3::conditional::{self, Current};
use mantle_s3::range::{self, Span};
use mantle_s3::seal::{DataKey, WrappingKey};

use super::{ChunkAsk, ChunkOutcome, Node, NodeError, NodeLog, PIECE, take_done};
use crate::ask::{CommandAsk, CommandOutcome, ReadAsk, ReadOutcome};
use crate::door::{self, Piece};
use crate::place;
use crate::read::{Query, Read};
use mantle_protocol::classes::Class;
use mantle_protocol::object::{
    self, Head, Headers, List, Listing, MAX_PAGE_PARTS, Op, PartListed, Parts, Progress, Reply,
    Status, Stored,
};

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
    /// The clients of the native protocol it admits at once, each by the certificate the cell's
    /// authority issued it for the client role (node.md §3.6). One past them is refused as the
    /// transport refuses an identity past its bound (`Refusal::Identities`), which the client asks
    /// again after.
    pub clients: usize,
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

/// An object operation the host runs: the door it came in by, its asker, and where it is.
pub(crate) struct Job {
    door: Door,
    /// Who asks: a native asker's peer id, or the account an S3 request's access key acts for.
    principal: String,
    work: Work,
    /// What the operation waits on, each with the driver's request it answers, if a driver's.
    pending: Vec<(Pending, Option<u64>)>,
}

/// Where a job's operation came in: its body is read and its reply written there.
enum Door {
    /// A native exchange, the operation's fields at its body's start (`mantle_protocol::object`).
    Exchange(ExchangeId),
    /// The S3 listener's, in-process (`crate::door`).
    Local(Box<Local>),
}

/// A local door's ends: the request's body as the listener sends it, and the reply as the host
/// writes it.
struct Local {
    body: ChannelReceiver<Piece>,
    reply: Sender<door::Answer>,
    /// The body's end once it came, with the checksum its trailer carried.
    ended: Option<Option<checksum::Checksum>>,
    /// Answers the reply's channel had no room for, sent before any other, in order. A stream's
    /// next piece is taken only once these are sent, so they are at most one piece of bytes and
    /// the few answers around it.
    pending: VecDeque<door::Answer>,
    /// Whether the listener was told the put wants its body.
    ready: bool,
}

/// What became of an answer sent on a local door.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sent {
    Taken,
    /// The channel is full: the answer is held, and sent first at a later turn.
    Held,
    /// The connection is gone.
    Gone,
}

impl Local {
    /// Sends what waits, in order: whether the door takes more now.
    fn flush(&mut self) -> Sent {
        while let Some(answer) = self.pending.pop_front() {
            match self.reply.try_send(answer) {
                Ok(()) => {}
                Err(SyncError::Full(answer)) => {
                    self.pending.push_front(answer);
                    return Sent::Held;
                }
                Err(SyncError::Closed(_) | SyncError::NotOnShardThread(_)) => return Sent::Gone,
            }
        }
        Sent::Taken
    }

    /// Sends `answer` after what waits, or holds it behind them.
    fn offer(&mut self, answer: door::Answer) -> Sent {
        self.pending.push_back(answer);
        self.flush()
    }
}

enum Work {
    /// The request's fields being read from its body's start: the head, the fields so far.
    Fields(Head, Vec<u8>),
    /// A request refused or done: what is left of its body read and dropped, then the reply, so
    /// the exchange ends whole (`Arriving::Redirect`'s reason). Boxed: a reply is many times any
    /// other job's state.
    Draining(Box<Reply>),
    /// A reply's head sent: the fields that open its body, and how much of them is written.
    Replying(Vec<u8>, usize),
    /// A local operation's last answers going out, in order: its reply and its end, or its end.
    Answering(VecDeque<door::Answer>),
    /// CreateBucket or DeleteBucket, through the bucket's coordinator.
    Bucket(Box<Coordinating>),
    Put(Box<Putting>),
    /// GetObject, or HeadObject.
    Get(Box<Getting>),
    /// DeleteObject, PutBucketVersioning, CreateMultipartUpload, a native upload's Open or
    /// AbortMultipartUpload: the bucket's row read, then one command made from it.
    Command(Box<Commanding>),
    /// CompleteMultipartUpload, or a native upload's Finish, run by the `Completion` driver.
    Complete(Box<Completing>),
    /// An operation one read answers.
    Read(Box<Single>),
}

/// A bucket's create or delete, run by its coordinator.
struct Coordinating {
    bucket: String,
    /// A DeleteBucket rather than a CreateBucket.
    delete: bool,
    /// A delete's bucket row read and its owner the asker: S3 lets no other principal delete a
    /// bucket unless a policy grants it (05 §10.5).
    owned: bool,
    coordinator: Option<Coordinator>,
}

struct Putting {
    bucket: String,
    key: String,
    /// An UploadPart's upload and number; a PutObject's none.
    part: Option<(String, u16)>,
    id: u128,
    length: u64,
    preconditions: Preconditions,
    /// What the body's end is checked against: its `Content-MD5` and checksum (05 §3.2).
    expected: Expected,
    /// The algorithm of the checksum a trailer brings with the body's end: aws-chunked's through
    /// the listener's door, or the value past a native put's bytes (`object::Op::Put`).
    trailing: Option<checksum::Algorithm>,
    /// A native put's trailer read so far, at most its algorithm's width.
    trailer: Vec<u8>,
    /// The driver, once the bucket's row was read.
    put: Option<Put>,
    /// Body bytes read and not yet taken by the driver, and the body's bytes taken so far.
    carry: Vec<u8>,
    taken: u64,
    /// The driver's requests not yet asked: the node held as many asks as it keeps.
    waiting: VecDeque<(u64, put::Request)>,
    /// A native upload's run (an Append): its upload's row read after its bucket's.
    run: Option<Appending>,
}

/// A native upload's run as its Append names it, and its upload's runs once read: where the run
/// begins, whether it ends the object, and the bucket's incarnation its commit is made under.
struct Appending {
    offset: u64,
    last: bool,
    incarnation: Option<u64>,
    runs: Option<Runs>,
}

struct Getting {
    bucket: String,
    key: String,
    headers: Headers,
    /// A HeadObject: the version's facts, and no bytes.
    head: bool,
    get: Option<Get>,
    /// Plaintext taken from the driver and not yet written to the exchange.
    out: Option<(Bytes, usize)>,
    /// Bytes of the reply's body written so far, and the body's length.
    written: u64,
    total: u64,
    waiting: VecDeque<(u64, get::Request)>,
}

/// An operation whose bucket's row is read first, for the incarnation, versioning and owner its
/// one command is made from.
struct Commanding {
    bucket: String,
    then: Then,
    /// The command asked.
    asked: bool,
}

/// The command a bucket's row is read for.
enum Then {
    /// DeleteObject: the Name range's delete (05 §7.3).
    Delete {
        key: String,
        if_match: Option<Match>,
    },
    /// PutBucketVersioning, by the bucket's owner (05 §7.1).
    Versioning(Versioning),
    /// CreateMultipartUpload, with the checksum its parts carry (05 §4.2).
    CreateUpload {
        key: String,
        checksum: Option<(Algorithm, bool)>,
    },
    /// A native upload's Open (gateway.md §2.1): its object's length where stated, its
    /// full-object checksum's algorithm, and the length of its runs (`name::run_length`).
    Open {
        key: String,
        length: Option<u64>,
        checksum: Algorithm,
        run: u64,
    },
    /// AbortMultipartUpload (05 §4.8).
    Abort { key: String, upload: String },
}

/// A CompleteMultipartUpload: its bucket's row read first, for the incarnation, versioning and
/// default retention its Complete names; then its driver.
struct Completing {
    bucket: String,
    key: String,
    upload: String,
    /// The parts the request lists, each its number and ETag.
    listed: Vec<(u16, String)>,
    /// A native upload's Finish: the length and full-object checksum its client sent, which its
    /// runs must combine to.
    native: Option<(u64, record::Checksum)>,
    preconditions: Preconditions,
    completion: Option<Completion>,
    /// The driver's requests not yet asked: the node held as many asks as it keeps.
    waiting: VecDeque<(u64, complete::Request)>,
}

/// An operation one read answers.
enum Single {
    /// HeadBucket: the bucket's row.
    HeadBucket(String),
    /// ListObjectsV2: a page of keys.
    List(List),
    /// ListBuckets: a page of the asker's buckets.
    Buckets {
        prefix: String,
        after: Option<String>,
        max: u32,
    },
    /// GetBucketVersioning: the bucket's row.
    Versioning(String),
    /// ListParts: a page of an upload's parts.
    Parts {
        bucket: String,
        key: String,
        upload: String,
        after: u16,
        max: u32,
    },
    /// ListMultipartUploads: a page of the bucket's uploads, its query made from the request.
    Uploads(Query),
    /// A native upload's Offset: its row, its progress.
    Offset {
        bucket: String,
        key: String,
        upload: String,
    },
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

/// The tags an `If-Match` (`strong`) or `If-None-Match` header names, as the Name range judges
/// them at commit (`mantle_s3::conditional::named`).
fn matched(header: Option<&str>, strong: bool) -> Option<Match> {
    header.map(|list| match conditional::named(list, strong) {
        None => Match::Any,
        Some(tags) => Match::Tags(tags),
    })
}

/// The status a Name range's refusal of a write is answered with (node.md §4.4).
fn name_status(outcome: &name::Outcome) -> Status {
    match outcome {
        name::Outcome::NoSuchBucket => Status::NoSuchBucket,
        name::Outcome::NoSuchKey => Status::NoSuchKey,
        name::Outcome::PreconditionFailed => Status::PreconditionFailed,
        name::Outcome::Locked => Status::AccessDenied,
        name::Outcome::NoSuchUpload => Status::NoSuchUpload,
        name::Outcome::InvalidPart => Status::InvalidPart,
        name::Outcome::InvalidPartOrder => Status::InvalidPartOrder,
        name::Outcome::EntityTooSmall => Status::EntityTooSmall,
        // A part's number or size outside S3's, which the Name range refuses too.
        name::Outcome::Invalid => Status::InvalidArgument,
        name::Outcome::Offset { .. } => Status::Offset,
        _ => Status::Failed,
    }
}

/// The status a completion's failure is answered with (05 §4.4).
fn complete_status(error: &CompleteError) -> Status {
    match error {
        CompleteError::NoSuchUpload => Status::NoSuchUpload,
        // Composite checksums need parts numbered from 1 without a gap: S3 answers 500 for the
        // gap, and mantle the part it cannot combine (05 §3.3).
        CompleteError::InvalidPart | CompleteError::NotConsecutive => Status::InvalidPart,
        CompleteError::InvalidPartOrder => Status::InvalidPartOrder,
        CompleteError::EntityTooSmall => Status::EntityTooSmall,
        CompleteError::BadDigest => Status::BadDigest,
        CompleteError::Unended => Status::IncompleteBody,
        CompleteError::Name(outcome) => name_status(outcome),
        // A listed part uploaded again while the completion ran: `500`, which clients retry.
        _ => Status::Failed,
    }
}

/// The status a put's failure is answered with (node.md §4.4).
fn put_status(error: &PutError) -> Status {
    match error {
        PutError::IncompleteBody => Status::IncompleteBody,
        PutError::EntityTooLarge { .. } => Status::EntityTooLarge,
        PutError::BadDigest | PutError::BadChecksum => Status::BadDigest,
        PutError::Unplaced(_) => Status::SlowDown,
        PutError::InvalidPartNumber(_) => Status::InvalidArgument,
        PutError::Name(outcome) => name_status(outcome),
        _ => Status::Failed,
    }
}

/// A put's checksum as its reply carries it: a full-object value (05 §3.3).
fn kept(c: &checksum::Checksum) -> Stored {
    Stored {
        algorithm: c.algorithm.code(),
        parts: 0,
        value: c.bytes.clone(),
    }
}

/// A part's row, its ETag, size, checksum, file and deadline the put's driver fills.
fn unfilled_part() -> Part {
    Part {
        etag: String::new(),
        size: 0,
        checksum: None,
        file: 0,
        modified_ns: 0,
        deadline_ns: 0,
    }
}

/// A native upload's progress as its runs stand, with a run's checksum in its frame.
fn progress(runs: &Runs, checksum: Option<Vec<u8>>) -> Progress {
    Progress {
        offset: runs.offset,
        runs: runs.count,
        run: runs.run,
        ended: runs.ended,
        checksum,
    }
}

/// A run's frame, once it committed: the offset after it, its number and its checksum.
fn run_frame(appending: &Appending, stored: &put::Stored) -> Option<Progress> {
    let runs = appending.runs?;
    Some(Progress {
        offset: appending.offset.checked_add(stored.size)?,
        runs: runs.count.checked_add(1)?,
        run: runs.run,
        ended: appending.last,
        checksum: stored.checksum.as_ref().map(|c| c.bytes.clone()),
    })
}

/// A version's checksum as a reply carries it.
fn shown(c: &record::Checksum) -> Stored {
    Stored {
        algorithm: c.algorithm,
        parts: c.parts,
        value: c.value.clone(),
    }
}

/// A bucket's versioning as GetBucketVersioning answers it: none for a bucket never versioned.
fn answered(versioning: Versioning) -> Option<mantle_s3::body::Versioning> {
    match versioning {
        Versioning::Unversioned => None,
        Versioning::Enabled => Some(mantle_s3::body::Versioning::Enabled),
        Versioning::Suspended => Some(mantle_s3::body::Versioning::Suspended),
    }
}

/// The reply that states a version's facts, `status` its answer.
fn facts(status: Status, order: u64, version: &Version) -> Reply {
    Reply {
        status,
        size: version.size,
        version: name::shown_id(version.null, order),
        etag: version.etag.clone(),
        modified: object::seconds(version.modified_ns),
        checksum: version.checksum.as_ref().map(shown),
        marker: version.marker,
        range: None,
        versioning: None,
        upload: String::new(),
        listing: None,
        progress: None,
    }
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// An object's request arrived on `exchange` from `peer`, its body `body` bytes: a job that
    /// reads its fields first, or the reply it gets at once.
    pub(super) fn take_object(&mut self, exchange: ExchangeId, peer: PeerId, body: Option<u64>) {
        // A client's operations act for the account its certificate names; one whose
        // connections have all gone since asking is no longer known, and nothing is taken.
        let Some(principal) = self.driver.endpoint().directory().principal(peer) else {
            self.driver
                .endpoint()
                .refuse_exchange(exchange, Refusal::Identity);
            return;
        };
        let head = Head::decode(
            self.driver.endpoint().head(exchange).unwrap_or_default(),
            body,
        );
        let work = match (head, self.gateway.as_ref()) {
            (_, None) => Work::Draining(Box::new(Reply::status(Status::Failed))),
            (Err(_), Some(_)) => Work::Draining(Box::new(Reply::status(Status::Malformed))),
            // A node stopping begins no operation (node.md §1.7): its client asks another.
            (Ok(_), Some(g)) if self.stopping() || self.jobs.len() >= g.objects => {
                Work::Draining(Box::new(Reply::status(Status::SlowDown)))
            }
            (Ok(head), Some(_)) => {
                let fields = usize::try_from(head.fields()).unwrap_or(0);
                Work::Fields(head, Vec::with_capacity(fields))
            }
        };
        self.jobs.push(Job {
            door: Door::Exchange(exchange),
            principal,
            work,
            pending: Vec::new(),
        });
    }

    /// An operation the S3 listener handed over (`crate::door`): a job of its work, or the reply
    /// it gets at once, as a native request's.
    pub(super) fn take_local(&mut self, request: door::Request) {
        let door::Request {
            op,
            principal,
            body,
            reply,
        } = request;
        let work = match self.gateway.as_ref() {
            None => Work::Draining(Box::new(Reply::status(Status::Failed))),
            Some(g) if self.stopping() || self.jobs.len() >= g.objects => {
                Work::Draining(Box::new(Reply::status(Status::SlowDown)))
            }
            Some(_) => work(op, Vec::new()),
        };
        self.jobs.push(Job {
            door: Door::Local(Box::new(Local {
                body,
                reply,
                ended: None,
                pending: VecDeque::new(),
                ready: false,
            })),
            principal,
            work,
            pending: Vec::new(),
        });
    }

    /// A job's fields read from its body's start, a piece at a time; once whole, the work they
    /// name, the bytes read past them the start of a put's body.
    fn read_fields(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        // A local operation comes typed, never as fields to read.
        let Door::Exchange(exchange) = job.door else {
            return Ok(true);
        };
        let Work::Fields(head, fields) = &mut job.work else {
            return Ok(false);
        };
        let wanted = usize::try_from(head.fields()).unwrap_or(usize::MAX);
        let endpoint = self.driver.endpoint();
        while fields.len() < wanted {
            let Ok(mut piece) = endpoint.reserve(Class::Transfer, PIECE) else {
                return Ok(false);
            };
            let got = endpoint.read_body(exchange, &mut piece);
            fields.extend_from_slice(piece.bytes());
            self.moved = self
                .moved
                .saturating_add(u64::try_from(piece.bytes().len()).unwrap_or(u64::MAX));
            endpoint.release(piece);
            match got {
                Ok(0) if fields.len() < wanted => return Ok(false),
                Ok(_) => {}
                // Its asker is gone.
                Err(_) => return Ok(true),
            }
        }
        let carry = fields.split_off(wanted.min(fields.len()));
        job.work = match head.op(fields) {
            Err(_) => Work::Draining(Box::new(Reply::status(Status::Malformed))),
            Ok(op) => work(op, carry),
        };
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
            Work::Fields(..) => self.read_fields(at),
            Work::Draining(_) => self.drain_job(at),
            Work::Replying(..) => self.write_fields(at),
            Work::Answering(_) => self.answer_local(at),
            Work::Bucket(_) => self.step_bucket(at),
            Work::Put(_) => self.step_put(at),
            Work::Get(_) => self.step_get(at),
            Work::Command(_) => self.step_command(at),
            Work::Complete(_) => self.step_complete(at),
            Work::Read(_) => self.step_single(at),
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
        let work = diag_work(&job.work);
        job.pending.retain(|(pending, request)| {
            let key = diag_key(pending);
            diag_seen(key);
            let outcome = match pending {
                Pending::Command(ask) => {
                    take_done(&mut self.commands_done, *ask).map(Outcome::Command)
                }
                Pending::Read(ask) => take_done(&mut self.reads_done, *ask).map(Outcome::Read),
                Pending::Chunk(ask) => take_done(&mut self.chunks_done, *ask).map(Outcome::Chunk),
            };
            match outcome {
                Some(outcome) => {
                    diag_taken(key, &outcome, work);
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
        let principal = job.principal.clone();
        match &mut job.work {
            Work::Fields(..) | Work::Draining(_) | Work::Replying(..) | Work::Answering(_) => {}
            // A native upload opened: its ID, and its progress, every run but the last as long
            // as the plan made it.
            Work::Command(commanding) if matches!(commanding.then, Then::Open { .. }) => {
                let Then::Open { run, .. } = commanding.then else {
                    return Ok(());
                };
                return match outcome {
                    Outcome::Read(ReadOutcome::Read(read)) => self.command_read(at, *read),
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(
                        name::Outcome::Created { upload },
                    ))) => {
                        let reply = Reply {
                            upload,
                            progress: Some(Box::new(Progress {
                                offset: 0,
                                runs: 0,
                                run,
                                ended: false,
                                checksum: None,
                            })),
                            ..Reply::status(Status::Done)
                        };
                        self.reply_job(at, reply)
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(refused))) => {
                        self.fail_job(at, name_status(&refused))
                    }
                    _ => self.fail_job(at, Status::Failed),
                };
            }
            Work::Bucket(coordinating) => {
                let answer = match outcome {
                    Outcome::Read(ReadOutcome::Read(read)) if !coordinating.owned => {
                        let status = match *read {
                            Read::Bucket(Some(row)) if row.owner == principal => None,
                            Read::Bucket(Some(_)) => Some(Status::AccessDenied),
                            Read::Bucket(None) => Some(Status::NoSuchBucket),
                            _ => Some(Status::Failed),
                        };
                        coordinating.owned = status.is_none();
                        return match status {
                            Some(status) => self.fail_job(at, status),
                            None => Ok(()),
                        };
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Bucket(o))) => {
                        if coordinating.coordinator.is_none() {
                            return self.begun(at, o);
                        }
                        coordinator::Answer::Bucket(o)
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => {
                        coordinator::Answer::Name(o)
                    }
                    Outcome::Read(ReadOutcome::Read(read)) => match *read {
                        Read::Gate(gate) => coordinator::Answer::Gate(gate),
                        Read::Probe(probe) => coordinator::Answer::Probe(probe),
                        _ => return self.fail_job(at, Status::Failed),
                    },
                    _ => return self.fail_job(at, Status::Failed),
                };
                let Some(c) = coordinating.coordinator.as_mut() else {
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
                    // An answer the driver refuses ends it with that outcome, which its next
                    // turn replies: a precondition or a lock the Name range judged, say.
                    if let Some(put) = putting.put.as_mut() {
                        let _ = put.answer(id, put_answer(outcome));
                    }
                }
            },
            Work::Get(getting) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.current_read(at, *read);
                }
                (None, _) => return self.fail_job(at, Status::Failed),
                (Some(id), outcome) => {
                    // An answer the driver refuses ends it with that outcome, which its next
                    // turn ends the exchange on.
                    if let Some(get) = getting.get.as_mut() {
                        let _ = get.answer(id, get_answer(outcome));
                    }
                }
            },
            Work::Command(_) => {
                return match outcome {
                    Outcome::Read(ReadOutcome::Read(read)) => self.command_read(at, *read),
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => {
                        let reply = match o {
                            name::Outcome::Deleted { marker, version } => Reply {
                                version: version.unwrap_or_default(),
                                marker,
                                ..Reply::status(Status::Done)
                            },
                            name::Outcome::Created { upload } => Reply {
                                upload,
                                ..Reply::status(Status::Done)
                            },
                            name::Outcome::Aborted => Reply::status(Status::Done),
                            refused => Reply::status(name_status(&refused)),
                        };
                        self.reply_job(at, reply)
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Bucket(o))) => {
                        let status = match o {
                            bucket::Outcome::Versioned => Status::Done,
                            bucket::Outcome::VersioningLocked => Status::InvalidBucketState,
                            bucket::Outcome::NoSuchBucket => Status::NoSuchBucket,
                            bucket::Outcome::OperationAborted => Status::OperationAborted,
                            _ => Status::Failed,
                        };
                        self.fail_job(at, status)
                    }
                    _ => self.fail_job(at, Status::Failed),
                };
            }
            Work::Complete(completing) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.complete_read(at, *read);
                }
                (None, _) => return self.fail_job(at, Status::Failed),
                (Some(id), outcome) => {
                    // A read or command refused, or answered with another kind: `500`, which
                    // clients retry; never the 404 an upload not found would be.
                    let Some(answer) = complete_answer(outcome) else {
                        return self.fail_job(at, Status::Failed);
                    };
                    // An answer the driver refuses ends it with that outcome, which its next
                    // turn replies.
                    if let Some(completion) = completing.completion.as_mut() {
                        let _ = completion.answer(id, answer);
                    }
                }
            },
            Work::Read(_) => {
                return match outcome {
                    Outcome::Read(ReadOutcome::Read(read)) => self.single_read(at, *read),
                    _ => self.fail_job(at, Status::Failed),
                };
            }
        }
        Ok(())
    }

    /// A bucket's create or delete answered by the Bucket range's first command: its
    /// coordinator, or the reply it gets.
    fn begun(&mut self, at: usize, outcome: bucket::Outcome) -> Result<(), NodeError> {
        let status = match outcome {
            bucket::Outcome::Creating { .. } | bucket::Outcome::Deleting { .. } => None,
            bucket::Outcome::AlreadyOwnedByYou => Some(Status::BucketAlreadyOwnedByYou),
            bucket::Outcome::AlreadyExists => Some(Status::BucketAlreadyExists),
            bucket::Outcome::TooManyBuckets => Some(Status::TooManyBuckets),
            bucket::Outcome::OperationAborted => Some(Status::OperationAborted),
            bucket::Outcome::NoSuchBucket => Some(Status::NoSuchBucket),
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
        let Some(Job {
            work: Work::Bucket(coordinating),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        match Coordinator::start(&coordinating.bucket, &outcome, &directory(), bounds) {
            Ok(Some(c)) => {
                coordinating.coordinator = Some(c);
                Ok(())
            }
            _ => self.fail_job(at, Status::Failed),
        }
    }

    /// A bucket's create or delete at a turn: a delete's bucket row read first, its owner checked;
    /// then its first command; then its coordinator's next request, one at a time; once the
    /// coordinator is done, its answer.
    fn step_bucket(&mut self, at: usize) -> Result<bool, NodeError> {
        let quota = self.gateway.as_ref().map_or(0, |g| g.bucket_quota);
        let Some(job) = self.jobs.get(at) else {
            return Ok(true);
        };
        if !job.pending.is_empty() {
            return Ok(false);
        }
        let Work::Bucket(coordinating) = &job.work else {
            return Ok(true);
        };
        let bucket = coordinating.bucket.clone();
        let next = match &coordinating.coordinator {
            None if coordinating.delete && !coordinating.owned => Next::Owner,
            None if coordinating.delete => {
                Next::Request(coordinator::Request::Bucket(bucket::Command::BeginDelete {
                    bucket: bucket.clone(),
                    at_ns: 0,
                }))
            }
            None => Next::Request(coordinator::Request::Bucket(bucket::Command::Create(
                bucket::Create {
                    bucket: bucket.clone(),
                    owner: job.principal.clone(),
                    location: String::new(),
                    at_ns: 0,
                    quota,
                    lock: false,
                },
            ))),
            Some(c) if c.is_done() => Next::Done(match (c.settled(), coordinating.delete) {
                (Some(Settled::Created), false) | (Some(Settled::Deleted), true) => Status::Done,
                (Some(Settled::NotEmpty), true) => Status::BucketNotEmpty,
                _ => Status::OperationAborted,
            }),
            Some(c) => match c.next() {
                Some(request) => Next::Request(request),
                None => Next::Done(Status::Failed),
            },
        };
        let (Some(bucket_group), Some(name_group)) =
            (self.group_of(Layer::Bucket), self.group_of(Layer::Name))
        else {
            self.fail_job(at, Status::Failed)?;
            return self.drain_job(at);
        };
        let asked = match next {
            Next::Done(status) => {
                self.fail_job(at, status)?;
                return self.drain_job(at);
            }
            Next::Owner => self
                .read(bucket_group, Query::Bucket { bucket }, u64::MAX)
                .map(Pending::Read),
            Next::Request(coordinator::Request::Bucket(command)) => self
                .command(bucket_group, Command::Bucket(command), u64::MAX)
                .map(Pending::Command),
            Next::Request(coordinator::Request::Name { command, .. }) => self
                .command(name_group, Command::Name(command), u64::MAX)
                .map(Pending::Command),
            Next::Request(coordinator::Request::Gate { generation, .. }) => self
                .read(name_group, Query::Gate { bucket, generation }, u64::MAX)
                .map(Pending::Read),
            Next::Request(coordinator::Request::Probe {
                generation,
                from,
                budget,
                ..
            }) => {
                let query = Query::Probe {
                    bucket,
                    generation,
                    from,
                    budget: u32::try_from(budget).unwrap_or(u32::MAX),
                };
                self.read(name_group, query, u64::MAX).map(Pending::Read)
            }
            // The cell's one Name range is the directory.
            Next::Request(coordinator::Request::Directory) => {
                if let Some(Job {
                    work: Work::Bucket(coordinating),
                    ..
                }) = self.jobs.get_mut(at)
                    && let Some(c) = coordinating.coordinator.as_mut()
                    && c.answer(coordinator::Answer::Directory(directory()))
                        .is_err()
                {
                    self.fail_job(at, Status::Failed)?;
                }
                return Ok(false);
            }
        };
        self.asked(at, asked, None)
    }

    /// What job `at` asked, kept to wait on; the asking's failure ends the job. An ask the node
    /// has no room for is asked again at the next turn.
    fn asked(
        &mut self,
        at: usize,
        asked: Result<Pending, NodeError>,
        request: Option<u64>,
    ) -> Result<bool, NodeError> {
        match asked {
            Ok(pending) => {
                if let Some(job) = self.jobs.get_mut(at) {
                    job.pending.push((pending, request));
                }
                Ok(false)
            }
            Err(NodeError::Asks) => Ok(false),
            Err(_) => {
                self.fail_job(at, Status::Failed)?;
                self.drain_job(at)
            }
        }
    }

    /// A put's bucket row, read: its driver made over it, or the reply `NoSuchBucket`. A native
    /// upload's run asks its upload's row next, which it is checked against (`run_read`).
    fn bucket_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let read = match read {
            Read::Upload(upload) => return self.run_read(at, upload),
            read => read,
        };
        let Read::Bucket(Some(row)) = read else {
            return self.fail_job(at, Status::NoSuchBucket);
        };
        if row.state != BucketState::Active {
            return self.fail_job(at, Status::NoSuchBucket);
        }
        if let Some(Job {
            work: Work::Put(putting),
            ..
        }) = self.jobs.get_mut(at)
            && let Some(appending) = putting.run.as_mut()
        {
            appending.incarnation = Some(row.created_ns);
            let query = Query::Upload {
                bucket: putting.bucket.clone(),
                key: putting.key.clone(),
                upload: putting
                    .part
                    .as_ref()
                    .map(|(upload, _)| upload.clone())
                    .unwrap_or_default(),
            };
            let Some(group) = self.group_of(Layer::Name) else {
                return self.fail_job(at, Status::Failed);
            };
            let asked = self.read(group, query, u64::MAX).map(Pending::Read);
            return self.asked(at, asked, None).map(|_| ());
        }
        let default = row.lock.and_then(|lock| lock.default);
        let Some(Job {
            work: Work::Put(putting),
            principal,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let commit = match &putting.part {
            // The part's ETag, size, checksum, file and deadline are the driver's to fill.
            Some((upload, number)) => Commit::Part(name::PutPart {
                bucket: putting.bucket.clone(),
                incarnation: row.created_ns,
                key: putting.key.clone(),
                upload: upload.clone(),
                number: *number,
                part: unfilled_part(),
                at_ns: 0,
                run: None,
            }),
            None => Commit::Object(name::Put {
                bucket: putting.bucket.clone(),
                incarnation: row.created_ns,
                key: putting.key.clone(),
                versioning: row.versioning,
                preconditions: std::mem::take(&mut putting.preconditions),
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
                    owner: principal.clone(),
                    headers: Vec::new(),
                    retention: None,
                    legal_hold: None,
                    listing: None,
                },
                default,
                id: putting.id,
                deadline_ns: 0,
            }),
        };
        self.start_put(at, commit)
    }

    /// A native upload's run, its upload's row read (gateway.md §2.1): begun at the offset the
    /// row holds, as long as the plan's runs or, the last, no longer, numbered the next; or the
    /// reply it earns, one at any other offset or after the last run `Offset`, naming the
    /// upload's.
    fn run_read(&mut self, at: usize, upload: Option<Upload>) -> Result<(), NodeError> {
        let Some(Job {
            work: Work::Put(putting),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let Some(upload) = upload else {
            return self.fail_job(at, Status::NoSuchUpload);
        };
        // An upload CreateMultipartUpload made takes its client's parts, never runs.
        let (Some(runs), Some(appending)) = (upload.runs, putting.run.as_mut()) else {
            return self.fail_job(at, Status::InvalidRequest);
        };
        appending.runs = Some(runs);
        if runs.ended || appending.offset != runs.offset {
            let reply = Reply {
                progress: Some(Box::new(progress(&runs, None))),
                ..Reply::status(Status::Offset)
            };
            return self.reply_job(at, reply);
        }
        let (offset, last) = (appending.offset, appending.last);
        let Some(incarnation) = appending.incarnation else {
            return self.fail_job(at, Status::Failed);
        };
        // The run's checksum is in the algorithm its object is held to at its end.
        if putting.trailing.map(Algorithm::code) != upload.checksum.map(|(code, _)| code) {
            return self.fail_job(at, Status::InvalidRequest);
        }
        let end = offset.checked_add(putting.length);
        let planned = match end {
            None => false,
            Some(end) if last => {
                putting.length <= runs.run && runs.length.is_none_or(|length| end == length)
            }
            Some(end) => {
                putting.length == runs.run && runs.length.is_none_or(|length| end < length)
            }
        };
        let number = runs
            .count
            .checked_add(1)
            .filter(|n| *n <= name::MAX_PART_NUMBER);
        let (true, Some(number), Some((upload_id, numbered))) =
            (planned, number, putting.part.as_mut())
        else {
            return self.fail_job(at, Status::InvalidArgument);
        };
        *numbered = number;
        let commit = Commit::Run {
            part: name::PutPart {
                bucket: putting.bucket.clone(),
                incarnation,
                key: putting.key.clone(),
                upload: upload_id.clone(),
                number,
                part: unfilled_part(),
                at_ns: 0,
                run: Some(name::Run {
                    offset,
                    last,
                    md5: runs.md5,
                }),
            },
            // The run's own MD5, its part's ETag, for the multipart ETag of an object that may
            // pass one PUT's bytes.
            own: runs
                .length
                .is_none_or(|length| length > mantle_s3::body::MAX_UPLOAD),
        };
        self.start_put(at, commit)
    }

    /// A put's driver, made over `commit`: its file's ID and data key drawn, under the root key's
    /// current generation, and its blocks laid out by the scheme new blocks take; or the reply
    /// its failure earns.
    fn start_put(&mut self, at: usize, commit: Commit) -> Result<(), NodeError> {
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
        let Some(Job {
            work: Work::Put(putting),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let body = Body {
            length: putting.length,
            checksum: putting
                .expected
                .checksum
                .as_ref()
                .map(|c| c.algorithm)
                .or(putting.trailing),
            content_md5: putting.expected.md5.is_some(),
        };
        match Put::new(commit, body, keys, layout, holding, Box::new(Random), now) {
            Ok(put) => {
                putting.put = Some(put);
                Ok(())
            }
            Err(error) => {
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
                    let run = putting.run.as_ref();
                    let over = put.outcome().map(|outcome| match outcome {
                        Ok(stored) => Reply {
                            version: stored.version.clone().unwrap_or_default(),
                            etag: stored.etag.clone(),
                            size: stored.size,
                            checksum: stored.checksum.as_ref().map(kept),
                            // A run's reply is its progress frame.
                            progress: run.and_then(|r| run_frame(r, stored)).map(Box::new),
                            ..Reply::status(Status::Done)
                        },
                        // Another run committed at its offset first: the offset it left.
                        Err(PutError::Name(name::Outcome::Offset { offset, ended })) => Reply {
                            progress: run.and_then(|r| r.runs).map(|runs| {
                                Box::new(Progress {
                                    offset: *offset,
                                    ended: *ended,
                                    ..progress(&runs, None)
                                })
                            }),
                            ..Reply::status(Status::Offset)
                        },
                        Err(error) => {
                            use std::io::Write as _;
                            let _ = writeln!(std::io::stderr(), "DIAG put ended {error:?}");
                            Reply::status(put_status(error))
                        }
                    });
                    match over {
                        Some(reply) => Step::Over(Box::new(reply)),
                        None if put.tick(wall).is_err() => {
                            Step::Over(Box::new(Reply::status(Status::Failed)))
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
                let asked = self
                    .read(group, Query::Bucket { bucket }, u64::MAX)
                    .map(Pending::Read);
                return self.asked(at, asked, None);
            }
            Step::Go => {}
        }
        self.feed_put(at)?;
        // The driver's requests, each asked of what serves it.
        loop {
            let Some(job) = self.jobs.get_mut(at) else {
                return Ok(true);
            };
            let Work::Put(putting) = &mut job.work else {
                return self.moved_on(at);
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
    /// last piece first, then a piece at a time; at the body's end, the body ended, its digests
    /// checked against what the request said before anything commits.
    fn feed_put(&mut self, at: usize) -> Result<(), NodeError> {
        if self
            .jobs
            .get(at)
            .is_some_and(|j| matches!(j.door, Door::Local(_)))
        {
            return self.feed_local_put(at);
        }
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(());
        };
        let Door::Exchange(exchange) = job.door else {
            return Ok(());
        };
        let Work::Put(putting) = &mut job.work else {
            return Ok(());
        };
        let Some(put) = putting.put.as_mut() else {
            return Ok(());
        };
        let endpoint = self.driver.endpoint();
        // A trailer's value follows the object's bytes: the driver is fed those alone.
        let width = putting.trailing.map_or(0, |a| a.width());
        loop {
            let left = usize::try_from(putting.length.saturating_sub(putting.taken))
                .unwrap_or(usize::MAX)
                .min(putting.carry.len());
            if left > 0 {
                if !put.wants_body() {
                    return Ok(());
                }
                let took = match putting.carry.get(..left).map(|bytes| put.feed(bytes)) {
                    Some(Ok(took)) => took,
                    _ => return self.fail_job(at, Status::Failed),
                };
                putting.taken = putting
                    .taken
                    .saturating_add(u64::try_from(took).unwrap_or(u64::MAX));
                putting.carry.drain(..took.min(putting.carry.len()));
                if took < left {
                    return Ok(());
                }
            }
            if putting.taken >= putting.length && !putting.carry.is_empty() {
                // Past the object's bytes, the trailer's; the transport holds the body to the
                // length its head stated, the trailer's width within it.
                if putting.trailer.len().saturating_add(putting.carry.len()) > width {
                    return self.fail_job(at, Status::Malformed);
                }
                putting.trailer.append(&mut putting.carry);
            }
            if putting.taken >= putting.length && putting.trailer.len() == width {
                if endpoint.body_complete(exchange) || (putting.length == 0 && width == 0) {
                    if let Some(algorithm) = putting.trailing {
                        putting.expected.checksum = Some(checksum::Checksum {
                            algorithm,
                            bytes: std::mem::take(&mut putting.trailer),
                        });
                    }
                    if put.end(&putting.expected).is_err() && put.outcome().is_none() {
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
            if putting.taken < putting.length && !put.wants_body() {
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

    /// A put's body from a local door (`crate::door`): the listener told once the driver first
    /// wants a byte, so a client that waits for `100 Continue` sends none before every check
    /// that could refuse it passed; each piece fed to the driver as it wants it, only what it did
    /// not take kept; and the body's end taken only once the listener checked its signature and
    /// its digest, its trailer's checksum then the one the end is checked against.
    fn feed_local_put(&mut self, at: usize) -> Result<(), NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(());
        };
        let (Door::Local(local), Work::Put(putting)) = (&mut job.door, &mut job.work) else {
            return Ok(());
        };
        let Some(put) = putting.put.as_mut() else {
            return Ok(());
        };
        if !local.ready && put.wants_body() {
            local.ready = true;
            if local.offer(door::Answer::Ready) == Sent::Gone {
                return self.fail_job(at, Status::IncompleteBody);
            }
        }
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
            if let Some(trailer) = local.ended.take() {
                // The listener framed the body at its stated length: an end before or past it is
                // a body that does not hold.
                if putting.taken != putting.length {
                    return self.fail_job(at, Status::IncompleteBody);
                }
                if let Some(checksum) = trailer {
                    putting.expected.checksum = Some(checksum);
                }
                if put.end(&putting.expected).is_err() && put.outcome().is_none() {
                    return self.fail_job(at, Status::Failed);
                }
                // Ended once: nothing more is read, and the outcome answers it.
                putting.length = u64::MAX;
                return Ok(());
            }
            if putting.length == u64::MAX || !put.wants_body() {
                return Ok(());
            }
            match local.body.try_recv() {
                Ok(Some(Piece::Bytes(bytes))) => {
                    let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                    self.moved = self.moved.saturating_add(len);
                    let took = match put.feed(&bytes) {
                        Ok(took) => took,
                        Err(_) => return self.fail_job(at, Status::Failed),
                    };
                    putting.taken = putting
                        .taken
                        .saturating_add(u64::try_from(took).unwrap_or(u64::MAX));
                    if let Some(rest) = bytes.get(took..) {
                        putting.carry.extend_from_slice(rest);
                    }
                }
                Ok(Some(Piece::End(trailer))) => local.ended = Some(trailer),
                Ok(Some(Piece::Refused(status))) => return self.fail_job(at, status),
                Ok(None) => return Ok(()),
                // The connection is gone with the body unfinished.
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
                // Refused, the driver ends with that outcome, which its next turn replies.
                if let Some(Job {
                    work: Work::Put(putting),
                    ..
                }) = self.jobs.get_mut(at)
                    && let Some(put) = putting.put.as_mut()
                {
                    let _ = put.answer(id, put::Answer::Volumes(volumes));
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
                    {
                        let _ = put.answer(id, put::Answer::Refused);
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

    /// A get's or head's current version, read: its preconditions judged and its range resolved
    /// against it (05 §2.4, §12); then a head's facts replied, or a get's driver made over the
    /// range and the reply's head sent.
    fn current_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let (order, version) = match read {
            Read::NoBucket => return self.fail_job(at, Status::NoSuchBucket),
            Read::Current(None) => return self.fail_job(at, Status::NoSuchKey),
            Read::Current(Some(found)) => found,
            _ => return self.fail_job(at, Status::Failed),
        };
        let Some(gateway) = self.gateway.as_ref() else {
            return self.fail_job(at, Status::Failed);
        };
        let window = gateway.window;
        let keyring = RootKeys {
            keys: gateway
                .keys
                .iter()
                .map(|(g, key)| (*g, *key.bytes()))
                .collect(),
        };
        let Some(Job {
            work: Work::Get(getting),
            door,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let mut reply = facts(Status::Done, order, &version);
        let current = if version.marker {
            Current::DeleteMarker
        } else {
            Current::Present {
                etag: &version.etag,
                modified: i64::try_from(reply.modified).unwrap_or(i64::MAX),
            }
        };
        reply.status = match conditional::read(&getting.headers.conditions(), &current) {
            conditional::Outcome::Proceed => Status::Done,
            // A current delete marker is no object: 404 with `x-amz-delete-marker` (05 §7.4).
            conditional::Outcome::NotFound => Status::NoSuchKey,
            conditional::Outcome::NotModified => Status::NotModified,
            conditional::Outcome::Failed => Status::PreconditionFailed,
        };
        if reply.status != Status::Done {
            if reply.status == Status::PreconditionFailed {
                reply = Reply::status(Status::PreconditionFailed);
            }
            return self.reply_job(at, reply);
        }
        let size = version.size;
        let served = match range::span(getting.headers.range.as_deref(), size) {
            Span::Whole => 0..size,
            Span::Part { first, last } => {
                reply.range = Some((first, last));
                first..last.saturating_add(1)
            }
            Span::Unsatisfiable => {
                let refused = Reply {
                    size,
                    ..Reply::status(Status::InvalidRange)
                };
                return self.reply_job(at, refused);
            }
        };
        if getting.head {
            return self.reply_job(at, reply);
        }
        let length = served.end.saturating_sub(served.start);
        match Get::new(version.file, size, served, Box::new(keyring), window) {
            Ok(get) => getting.get = Some(get),
            Err(_) => return self.fail_job(at, Status::Failed),
        }
        let exchange = match door {
            Door::Exchange(exchange) => *exchange,
            // The reply goes first, its bytes after it as the driver gives them (`write_get`).
            Door::Local(local) => {
                getting.total = length;
                if local.offer(door::Answer::Head(Box::new(reply))) == Sent::Gone {
                    return self.fail_job(at, Status::Failed);
                }
                return Ok(());
            }
        };
        let Ok((head, fields)) = reply.encode() else {
            return self.fail_job(at, Status::Failed);
        };
        // The fields open the body, ahead of the range's bytes.
        let total = u64::try_from(fields.len())
            .unwrap_or(u64::MAX)
            .saturating_add(length);
        getting.total = total;
        getting.out = Some((Bytes::from(fields), 0));
        match self.driver.endpoint().reply(exchange, &head, Some(total)) {
            Ok(()) => Ok(()),
            // Its asker is gone: the job ends with its exchange.
            Err(_) => self.fail_job(at, Status::Failed),
        }
    }

    /// A get's or head's turn: its current version read first; then a get's driver's requests
    /// asked and its bytes written as far as the exchange takes them; once every byte is written,
    /// its end.
    fn step_get(&mut self, at: usize) -> Result<bool, NodeError> {
        let name = self.group_of(Layer::Name);
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let Work::Get(getting) = &mut job.work else {
            return Ok(true);
        };
        if getting.get.is_none() {
            if !job.pending.is_empty() {
                return Ok(false);
            }
            let query = Query::Current {
                bucket: getting.bucket.clone(),
                key: getting.key.clone(),
            };
            let Some(group) = name else {
                self.fail_job(at, Status::Failed)?;
                return self.drain_job(at);
            };
            let asked = self.read(group, query, u64::MAX).map(Pending::Read);
            return self.asked(at, asked, None);
        }
        if self.write_get(at)? {
            return Ok(true);
        }
        loop {
            let Some(job) = self.jobs.get_mut(at) else {
                return Ok(true);
            };
            let Work::Get(getting) = &mut job.work else {
                return self.moved_on(at);
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
        let Work::Get(getting) = &mut job.work else {
            return Ok(true);
        };
        let Some(get) = getting.get.as_mut() else {
            return Ok(false);
        };
        let exchange = match &mut job.door {
            Door::Exchange(exchange) => *exchange,
            Door::Local(local) => {
                // A piece is taken from the driver only once the last is sent, so the channel holds
                // what the client has yet to read and the driver the rest of its window.
                loop {
                    match local.flush() {
                        Sent::Taken => {}
                        Sent::Held => return Ok(false),
                        Sent::Gone => return Ok(true),
                    }
                    let Some(bytes) = get.take() else {
                        break;
                    };
                    let took = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                    getting.written = getting.written.saturating_add(took);
                    self.moved = self.moved.saturating_add(took);
                    if local.offer(door::Answer::Bytes(bytes)) == Sent::Gone {
                        return Ok(true);
                    }
                }
                let last = match get.outcome() {
                    Some(Ok(())) if getting.written >= getting.total => door::Answer::End,
                    // The driver failed past the reply's head: the response ends unfinished.
                    Some(Err(_)) => door::Answer::Failed,
                    _ => return Ok(false),
                };
                if local.offer(last) == Sent::Held {
                    // The last answer waits for room: the job sends it before it ends.
                    job.work = Work::Answering(VecDeque::new());
                    return Ok(false);
                }
                return Ok(true);
            }
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
                    // A volume the table no longer holds: another copy is read, or the driver
                    // ends with its outcome.
                    if let Some(Job {
                        work: Work::Get(getting),
                        ..
                    }) = self.jobs.get_mut(at)
                        && let Some(get) = getting.get.as_mut()
                    {
                        let _ = get.answer(id, get::Answer::Unreadable);
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

    /// A commanding operation's turn: its bucket's row read first, then its command; the
    /// command's answer replies (`answer_job`).
    fn step_command(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(Job {
            work: Work::Command(commanding),
            pending,
            ..
        }) = self.jobs.get(at)
        else {
            return Ok(true);
        };
        if !pending.is_empty() || commanding.asked {
            return Ok(false);
        }
        let Some(group) = self.group_of(Layer::Bucket) else {
            self.fail_job(at, Status::Failed)?;
            return self.drain_job(at);
        };
        let query = Query::Bucket {
            bucket: commanding.bucket.clone(),
        };
        let asked = self.read(group, query, u64::MAX).map(Pending::Read);
        self.asked(at, asked, None)
    }

    /// A commanding operation's bucket row, read: its command asked, a delete's under the
    /// bucket's incarnation and versioning (05 §7.3), a versioning change by the bucket's owner;
    /// or the reply `NoSuchBucket` or `AccessDenied`.
    fn command_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let Read::Bucket(Some(row)) = read else {
            return self.fail_job(at, Status::NoSuchBucket);
        };
        if row.state != BucketState::Active {
            return self.fail_job(at, Status::NoSuchBucket);
        }
        let (Some(bucket_group), Some(name_group)) =
            (self.group_of(Layer::Bucket), self.group_of(Layer::Name))
        else {
            return self.fail_job(at, Status::Failed);
        };
        let Some(Job {
            work: Work::Command(commanding),
            principal,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let (group, command) = match &mut commanding.then {
            Then::Delete { key, if_match } => (
                name_group,
                Command::Name(Box::new(name::Command::Delete(name::Delete {
                    bucket: commanding.bucket.clone(),
                    incarnation: row.created_ns,
                    key: key.clone(),
                    versioning: row.versioning,
                    named: None,
                    if_match: if_match.take(),
                    at_ns: 0,
                    bypass: false,
                    owner: principal.clone(),
                }))),
            ),
            // S3 lets no other principal change a bucket's versioning unless a policy grants it.
            Then::Versioning(_) if row.owner != *principal => {
                return self.fail_job(at, Status::AccessDenied);
            }
            Then::Versioning(versioning) => (
                bucket_group,
                Command::Bucket(bucket::Command::Version {
                    bucket: commanding.bucket.clone(),
                    incarnation: row.created_ns,
                    versioning: *versioning,
                }),
            ),
            Then::CreateUpload { key, checksum } => (
                name_group,
                Command::Name(Box::new(name::Command::CreateUpload(name::CreateUpload {
                    bucket: commanding.bucket.clone(),
                    incarnation: row.created_ns,
                    key: key.clone(),
                    at_ns: 0,
                    upload: Upload {
                        initiated_ns: 0,
                        owner: principal.clone(),
                        headers: Vec::new(),
                        checksum: checksum.map(|(algorithm, full)| (algorithm.code(), full)),
                        retention: None,
                        legal_hold: None,
                        runs: None,
                    },
                }))),
            ),
            Then::Open {
                key,
                length,
                checksum,
                run,
            } => {
                // The MD5 across the runs is kept while the object may be one PUT's bytes, its
                // ETag then (gateway.md §2.1), begun from MD5's initial state.
                let md5 = if length.is_none_or(|l| l <= mantle_s3::body::MAX_UPLOAD) {
                    let begun = mantle_s3::crypto::Digest::new(&mantle_s3::crypto::MD5)
                        .and_then(|d| d.state())
                        .ok()
                        .and_then(|state| <[u8; 16]>::try_from(state.chaining()).ok());
                    match begun {
                        Some(chaining) => Some(RunMd5::Running(chaining)),
                        None => return self.fail_job(at, Status::Failed),
                    }
                } else {
                    None
                };
                (
                    name_group,
                    Command::Name(Box::new(name::Command::CreateUpload(name::CreateUpload {
                        bucket: commanding.bucket.clone(),
                        incarnation: row.created_ns,
                        key: key.clone(),
                        at_ns: 0,
                        upload: Upload {
                            initiated_ns: 0,
                            owner: principal.clone(),
                            headers: Vec::new(),
                            checksum: Some((checksum.code(), true)),
                            retention: None,
                            legal_hold: None,
                            runs: Some(Runs {
                                length: *length,
                                run: *run,
                                offset: 0,
                                count: 0,
                                ended: false,
                                md5,
                            }),
                        },
                    }))),
                )
            }
            Then::Abort { key, upload } => (
                name_group,
                Command::Name(Box::new(name::Command::Abort(name::Abort {
                    bucket: commanding.bucket.clone(),
                    incarnation: row.created_ns,
                    key: key.clone(),
                    upload: upload.clone(),
                    at_ns: 0,
                }))),
            ),
        };
        commanding.asked = true;
        let asked = self.command(group, command, u64::MAX).map(Pending::Command);
        self.asked(at, asked, None).map(|_| ())
    }

    /// A completion's turn: its bucket's row read first; then its driver's requests asked; once
    /// its outcome is known, its answer.
    fn step_complete(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(Job {
            work: Work::Complete(completing),
            pending,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(true);
        };
        let Some(completion) = completing.completion.as_mut() else {
            if !pending.is_empty() {
                return Ok(false);
            }
            let query = Query::Bucket {
                bucket: completing.bucket.clone(),
            };
            let Some(group) = self.group_of(Layer::Bucket) else {
                self.fail_job(at, Status::Failed)?;
                return self.drain_job(at);
            };
            let asked = self.read(group, query, u64::MAX).map(Pending::Read);
            return self.asked(at, asked, None);
        };
        if let Some(outcome) = completion.outcome() {
            let reply = match outcome {
                Ok(done) => Reply {
                    version: done.version.clone(),
                    etag: done.etag.clone(),
                    size: done.size,
                    checksum: done.checksum.as_ref().map(shown),
                    ..Reply::status(Status::Done)
                },
                Err(error) => {
                    use std::io::Write as _;
                    let _ = writeln!(std::io::stderr(), "DIAG completion ended {error:?}");
                    Reply::status(complete_status(error))
                }
            };
            self.reply_job(at, reply)?;
            return self.drain_job(at);
        }
        loop {
            let Some(job) = self.jobs.get_mut(at) else {
                return Ok(true);
            };
            let Work::Complete(completing) = &mut job.work else {
                return self.moved_on(at);
            };
            let next = match completing.waiting.pop_front() {
                Some(request) => Some(request),
                None => completing.completion.as_mut().and_then(Completion::poll),
            };
            let Some((id, request)) = next else {
                break;
            };
            if let Some(request) = self.ask_complete(at, id, request)? {
                if let Some(Job {
                    work: Work::Complete(completing),
                    ..
                }) = self.jobs.get_mut(at)
                {
                    completing.waiting.push_front((id, request));
                }
                break;
            }
        }
        Ok(false)
    }

    /// A completion's bucket row, read: its driver made over it, or the reply `NoSuchBucket` or
    /// the refusal its parts' list earns before anything is asked (05 §4.4).
    fn complete_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let Read::Bucket(Some(row)) = read else {
            return self.fail_job(at, Status::NoSuchBucket);
        };
        if row.state != BucketState::Active {
            return self.fail_job(at, Status::NoSuchBucket);
        }
        let Some(handover_ns) = self.gateway.as_ref().map(|g| g.handover_ns) else {
            return self.fail_job(at, Status::Failed);
        };
        let now = self.wall.now_ns();
        let mut file = [0u8; 16];
        if getrandom::fill(&mut file).is_err() {
            return self.fail_job(at, Status::Failed);
        }
        let Some(Job {
            work: Work::Complete(completing),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        // What the Complete says of the object, its parts, ETag, size, checksum, file, listing,
        // ID and deadline, the driver derives from the upload's rows.
        let complete = name::Complete {
            bucket: completing.bucket.clone(),
            incarnation: row.created_ns,
            key: completing.key.clone(),
            upload: completing.upload.clone(),
            versioning: row.versioning,
            preconditions: std::mem::take(&mut completing.preconditions),
            at_ns: 0,
            parts: Vec::new(),
            etag: String::new(),
            size: 0,
            checksum: None,
            file: None,
            listing: [0; LISTING],
            default: row.lock.and_then(|lock| lock.default),
            id: 0,
            deadline_ns: 0,
            native: false,
        };
        let listed = std::mem::take(&mut completing.listed);
        let made = match completing.native.take() {
            // A native upload's Finish: every run, held to what its client sent.
            Some(sent) => {
                Completion::native(complete, sent, u128::from_be_bytes(file), handover_ns, now)
            }
            None => Completion::new(
                complete,
                listed,
                u128::from_be_bytes(file),
                handover_ns,
                now,
            ),
        };
        match made {
            Ok(completion) => {
                completing.completion = Some(completion);
                Ok(())
            }
            Err(error) => {
                let status = complete_status(&error);
                self.fail_job(at, status)
            }
        }
    }

    /// Asks a completion's driver's request of what serves it; the request back when the node
    /// holds as many asks as it keeps.
    fn ask_complete(
        &mut self,
        at: usize,
        id: u64,
        request: complete::Request,
    ) -> Result<Option<complete::Request>, NodeError> {
        let Some(Job {
            work: Work::Complete(completing),
            ..
        }) = self.jobs.get(at)
        else {
            return Ok(None);
        };
        let (bucket, key, upload) = (
            completing.bucket.clone(),
            completing.key.clone(),
            completing.upload.clone(),
        );
        let asked = match &request {
            complete::Request::Upload => self
                .group_of(Layer::Name)
                .ok_or(NodeError::Bounds("no Name range here".into()))
                .and_then(|g| {
                    let query = Query::Upload {
                        bucket,
                        key,
                        upload,
                    };
                    self.read(g, query, u64::MAX)
                })
                .map(Pending::Read),
            complete::Request::Parts { after, max } => self
                .group_of(Layer::Name)
                .ok_or(NodeError::Bounds("no Name range here".into()))
                .and_then(|g| {
                    let query = Query::Parts {
                        bucket,
                        key,
                        upload,
                        after: *after,
                        max: u32::try_from(*max).unwrap_or(u32::MAX),
                    };
                    self.read(g, query, u64::MAX)
                })
                .map(Pending::Read),
            complete::Request::File(command) => self
                .group_of(Layer::File)
                .ok_or(NodeError::Bounds("no File range here".into()))
                .and_then(|g| self.command(g, Command::File(command.clone()), u64::MAX))
                .map(Pending::Command),
            complete::Request::Name(command) => self
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

    /// An operation one read answers, at its turn: its read asked once.
    fn step_single(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get(at) else {
            return Ok(true);
        };
        if !job.pending.is_empty() {
            return Ok(false);
        }
        let Work::Read(single) = &job.work else {
            return Ok(true);
        };
        let (layer, query) = match single.as_ref() {
            Single::HeadBucket(bucket) => (
                Layer::Bucket,
                Query::Bucket {
                    bucket: bucket.clone(),
                },
            ),
            Single::List(list) => (Layer::Name, Query::List(list.clone())),
            Single::Buckets { prefix, after, max } => (
                Layer::Bucket,
                Query::Owned {
                    owner: job.principal.clone(),
                    prefix: prefix.clone(),
                    after: after.clone(),
                    max: *max,
                },
            ),
            Single::Versioning(bucket) => (
                Layer::Bucket,
                Query::Bucket {
                    bucket: bucket.clone(),
                },
            ),
            Single::Parts {
                bucket,
                key,
                upload,
                after,
                max,
            } => (
                Layer::Name,
                Query::Parts {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    upload: upload.clone(),
                    after: *after,
                    max: *max,
                },
            ),
            Single::Uploads(query) => (Layer::Name, query.clone()),
            Single::Offset {
                bucket,
                key,
                upload,
            } => (
                Layer::Name,
                Query::Upload {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    upload: upload.clone(),
                },
            ),
        };
        let Some(group) = self.group_of(layer) else {
            self.fail_job(at, Status::Failed)?;
            return self.drain_job(at);
        };
        let asked = self.read(group, query, u64::MAX).map(Pending::Read);
        self.asked(at, asked, None)
    }

    /// The answer of an operation one read answers: its reply.
    fn single_read(&mut self, at: usize, read: Read) -> Result<(), NodeError> {
        let Some(job) = self.jobs.get(at) else {
            return Ok(());
        };
        let reply = match (&job.work, read) {
            (Work::Read(single), Read::Bucket(row)) => match row {
                Some(row) if row.state != BucketState::Active => {
                    Reply::status(Status::NoSuchBucket)
                }
                // S3 answers another principal's bucket 403 unless a policy grants it.
                Some(row) if row.owner != job.principal => Reply::status(Status::AccessDenied),
                Some(row) => match **single {
                    Single::Versioning(_) => Reply {
                        versioning: answered(row.versioning),
                        ..Reply::status(Status::Done)
                    },
                    _ => Reply::status(Status::Done),
                },
                None => Reply::status(Status::NoSuchBucket),
            },
            (_, Read::NoBucket) => Reply::status(Status::NoSuchBucket),
            (_, Read::Keys(page)) => Reply {
                listing: Some(Listing::Keys(page)),
                ..Reply::status(Status::Done)
            },
            (_, Read::Owned(page)) => Reply {
                listing: Some(Listing::Buckets(page)),
                ..Reply::status(Status::Done)
            },
            (_, Read::Parts(None)) => Reply::status(Status::NoSuchUpload),
            (_, Read::Parts(Some((parts, more)))) => {
                let next = if more {
                    parts.last().map(|(number, _)| *number)
                } else {
                    None
                };
                let parts = parts
                    .into_iter()
                    .map(|(number, part)| PartListed {
                        number,
                        etag: part.etag,
                        size: part.size,
                        modified: object::seconds(part.modified_ns),
                        checksum: part.checksum,
                    })
                    .collect();
                Reply {
                    listing: Some(Listing::Parts(Parts {
                        parts,
                        truncated: more,
                        next,
                    })),
                    ..Reply::status(Status::Done)
                }
            }
            (_, Read::Uploads(page)) => Reply {
                listing: Some(Listing::Uploads(page)),
                ..Reply::status(Status::Done)
            },
            // A native upload's progress, which a resuming client appends from; an upload
            // CreateMultipartUpload made has none.
            (_, Read::Upload(Some(upload))) => match upload.runs {
                Some(runs) => Reply {
                    progress: Some(Box::new(progress(&runs, None))),
                    ..Reply::status(Status::Done)
                },
                None => Reply::status(Status::InvalidRequest),
            },
            (_, Read::Upload(None)) => Reply::status(Status::NoSuchUpload),
            _ => Reply::status(Status::Failed),
        };
        self.reply_job(at, reply)
    }

    /// Ends job `at` with `reply`: what it waits on is let go, and its body, if any is left, read
    /// to its end and dropped before the reply goes.
    fn reply_job(&mut self, at: usize, reply: Reply) -> Result<(), NodeError> {
        if let Some(job) = self.jobs.get_mut(at) {
            job.work = Work::Draining(Box::new(reply));
            self.abandoned
                .extend(job.pending.drain(..).map(|(pending, _)| pending));
        }
        Ok(())
    }

    /// A job a callee moved on within its turn, to draining a refusal (`reply_job`) or to answering
    /// its last answers while its door's channel has no room for them (`write_get`): that work's turn
    /// now, so a refusal's reply goes at once, and a job ends only once its last answer went;
    /// whether it is over. A step that found its job no longer its own kind and ended it dropped
    /// that reply, and a get's end its channel held: the listener then closed the connection under
    /// a response whose every byte it had sent.
    fn moved_on(&mut self, at: usize) -> Result<bool, NodeError> {
        match self.jobs.get(at).map(|job| &job.work) {
            None => Ok(true),
            Some(Work::Draining(_)) => self.drain_job(at),
            Some(Work::Answering(_)) => self.answer_local(at),
            Some(_) => Ok(false),
        }
    }

    /// Ends job `at` with `status` alone.
    #[track_caller]
    fn fail_job(&mut self, at: usize, status: Status) -> Result<(), NodeError> {
        use std::io::Write as _;
        let caller = std::panic::Location::caller();
        let _ = writeln!(std::io::stderr(), "DIAG fail_job {status:?} at {caller}");
        self.reply_job(at, Reply::status(status))
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
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let exchange = match job.door {
            Door::Exchange(exchange) => exchange,
            // What is left of the body is the listener's to read or drop (node.md §4.5): the reply
            // and its end go now.
            Door::Local(_) => {
                let Work::Draining(reply) =
                    std::mem::replace(&mut job.work, Work::Answering(VecDeque::new()))
                else {
                    return Ok(false);
                };
                job.work = Work::Answering(VecDeque::from([
                    door::Answer::Head(reply),
                    door::Answer::End,
                ]));
                return self.answer_local(at);
            }
        };
        let Work::Draining(reply) = &job.work else {
            return Ok(false);
        };
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
        let Ok((head, fields)) = reply.encode() else {
            return Ok(true);
        };
        let length = u64::try_from(fields.len()).unwrap_or(u64::MAX);
        if endpoint.reply(exchange, &head, Some(length)).is_err() {
            return Ok(true);
        }
        if let Some(job) = self.jobs.get_mut(at) {
            job.work = Work::Replying(fields, 0);
        }
        self.write_fields(at)
    }

    /// A reply's fields written as far as its exchange takes them; once all are, the exchange
    /// ends.
    fn write_fields(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let Door::Exchange(exchange) = job.door else {
            return Ok(true);
        };
        let Work::Replying(fields, written) = &mut job.work else {
            return Ok(false);
        };
        let endpoint = self.driver.endpoint();
        while let Some(rest) = fields.get(*written..)
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

    /// A local job's last answers sent as far as its channel takes them; whether it is over: every
    /// one sent, or the connection gone.
    fn answer_local(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let (Door::Local(local), Work::Answering(answers)) = (&mut job.door, &mut job.work) else {
            return Ok(true);
        };
        local.pending.extend(answers.drain(..));
        Ok(local.flush() != Sent::Held)
    }

    /// The jobs whose exchange is `exchange`, for an event on it.
    pub(super) fn job_on(&self, exchange: ExchangeId) -> Option<usize> {
        self.jobs
            .iter()
            .position(|j| matches!(j.door, Door::Exchange(e) if e == exchange))
    }

    /// An exchange of a job ended unanswered: the job ends, what it waits on let go.
    pub(super) fn job_lost(&mut self, exchange: ExchangeId) {
        let Self {
            jobs, abandoned, ..
        } = self;
        jobs.retain_mut(|j| {
            if !matches!(j.door, Door::Exchange(e) if e == exchange) {
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

/// The cell's first Name range, which spans every key at its first generation (node.md §1.5): the
/// directory a bucket's coordinator routes by.
fn directory() -> Vec<Descriptor> {
    vec![Descriptor {
        id: 1,
        lo: Vec::new(),
        hi: None,
        generation: 1,
    }]
}

/// The work an operation's request names, the bytes read past its fields the start of a put's
/// body.
fn work(op: Op, carry: Vec<u8>) -> Work {
    match op {
        Op::Create { bucket, .. } => Work::Bucket(Box::new(Coordinating {
            bucket,
            delete: false,
            owned: true,
            coordinator: None,
        })),
        Op::DeleteBucket { bucket, .. } => Work::Bucket(Box::new(Coordinating {
            bucket,
            delete: true,
            owned: false,
            coordinator: None,
        })),
        Op::Put { length, .. } if length > mantle_s3::body::MAX_UPLOAD => {
            Work::Draining(Box::new(Reply::status(Status::EntityTooLarge)))
        }
        // A body states one checksum at most, in its fields or its trailer (05 §3.2).
        Op::Put {
            checksum: Some(_),
            trailing: Some(_),
            ..
        }
        | Op::UploadPart {
            checksum: Some(_),
            trailing: Some(_),
            ..
        } => Work::Draining(Box::new(Reply::status(Status::InvalidRequest))),
        Op::Put {
            bucket,
            key,
            id,
            length,
            headers,
            checksum,
            md5,
            trailing,
        } => Work::Put(Box::new(Putting {
            bucket,
            key,
            part: None,
            id,
            length,
            preconditions: Preconditions {
                if_match: matched(headers.if_match.as_deref(), true),
                if_none_match: matched(headers.if_none_match.as_deref(), false),
            },
            expected: Expected { md5, checksum },
            trailing,
            trailer: Vec::new(),
            put: None,
            taken: 0,
            carry,
            waiting: VecDeque::new(),
            run: None,
        })),
        Op::Get {
            bucket,
            key,
            headers,
        } => getting(bucket, key, headers, false),
        Op::Head {
            bucket,
            key,
            headers,
        } => getting(bucket, key, headers, true),
        Op::Delete {
            bucket,
            key,
            headers,
            ..
        } => Work::Command(Box::new(Commanding {
            bucket,
            then: Then::Delete {
                key,
                if_match: matched(headers.if_match.as_deref(), true),
            },
            asked: false,
        })),
        Op::PutVersioning {
            bucket, versioning, ..
        } => Work::Command(Box::new(Commanding {
            bucket,
            then: Then::Versioning(match versioning {
                mantle_s3::body::Versioning::Enabled => Versioning::Enabled,
                mantle_s3::body::Versioning::Suspended => Versioning::Suspended,
            }),
            asked: false,
        })),
        Op::GetVersioning { bucket } => Work::Read(Box::new(Single::Versioning(bucket))),
        // A checksum type its algorithm does not have: a full-object hash or a composite
        // CRC-64/NVME (05 §3.3), refused before anything is written.
        Op::CreateUpload {
            checksum: Some((algorithm, full)),
            ..
        } if !(if full {
            algorithm.full_object()
        } else {
            algorithm.composite()
        }) =>
        {
            Work::Draining(Box::new(Reply::status(Status::InvalidRequest)))
        }
        Op::CreateUpload {
            bucket,
            key,
            checksum,
            ..
        } => Work::Command(Box::new(Commanding {
            bucket,
            then: Then::CreateUpload { key, checksum },
            asked: false,
        })),
        Op::UploadPart { length, .. } if length > name::MAX_PART => {
            Work::Draining(Box::new(Reply::status(Status::EntityTooLarge)))
        }
        Op::UploadPart {
            bucket,
            key,
            upload,
            number,
            id,
            length,
            checksum,
            md5,
            trailing,
        } => Work::Put(Box::new(Putting {
            bucket,
            key,
            part: Some((upload, number)),
            id,
            length,
            preconditions: Preconditions::default(),
            expected: Expected { md5, checksum },
            trailing,
            trailer: Vec::new(),
            put: None,
            taken: 0,
            carry,
            waiting: VecDeque::new(),
            run: None,
        })),
        Op::Complete {
            bucket,
            key,
            upload,
            headers,
            parts,
            ..
        } => Work::Complete(Box::new(Completing {
            bucket,
            key,
            upload,
            listed: parts,
            native: None,
            preconditions: Preconditions {
                if_match: matched(headers.if_match.as_deref(), true),
                if_none_match: matched(headers.if_none_match.as_deref(), false),
            },
            completion: None,
            waiting: VecDeque::new(),
        })),
        Op::Abort {
            bucket,
            key,
            upload,
            ..
        } => Work::Command(Box::new(Commanding {
            bucket,
            then: Then::Abort { key, upload },
            asked: false,
        })),
        // A native upload's object is held to a CRC, which combines from its runs' (05 §3.3).
        Op::Open { checksum, .. } if !checksum.full_object() => {
            Work::Draining(Box::new(Reply::status(Status::InvalidRequest)))
        }
        Op::Open {
            bucket,
            key,
            length,
            buffer,
            checksum,
            ..
        } => match name::run_length(length, buffer) {
            Some(run) => Work::Command(Box::new(Commanding {
                bucket,
                then: Then::Open {
                    key,
                    length,
                    checksum,
                    run,
                },
                asked: false,
            })),
            // No run fits: an object past what 10,000 runs of S3's largest part hold, or a
            // buffer below its least part or too small for the length's runs.
            None if length.is_some_and(|l| {
                l > name::MAX_PART.saturating_mul(u64::from(name::MAX_PART_NUMBER))
            }) =>
            {
                Work::Draining(Box::new(Reply::status(Status::EntityTooLarge)))
            }
            None => Work::Draining(Box::new(Reply::status(Status::InvalidArgument))),
        },
        // A run's checksum is its upload's, in a trailer, which the run is checked by.
        Op::Append { trailing: None, .. } => {
            Work::Draining(Box::new(Reply::status(Status::InvalidRequest)))
        }
        Op::Append { length, .. } if length > name::MAX_PART => {
            Work::Draining(Box::new(Reply::status(Status::EntityTooLarge)))
        }
        Op::Append {
            bucket,
            key,
            upload,
            id,
            offset,
            length,
            last,
            trailing,
        } => Work::Put(Box::new(Putting {
            bucket,
            key,
            // Numbered once its upload's row is read.
            part: Some((upload, 0)),
            id,
            length,
            preconditions: Preconditions::default(),
            expected: Expected::default(),
            trailing,
            trailer: Vec::new(),
            put: None,
            taken: 0,
            carry,
            waiting: VecDeque::new(),
            run: Some(Appending {
                offset,
                last,
                incarnation: None,
                runs: None,
            }),
        })),
        Op::Offset {
            bucket,
            key,
            upload,
        } => Work::Read(Box::new(Single::Offset {
            bucket,
            key,
            upload,
        })),
        Op::Finish {
            bucket,
            key,
            upload,
            headers,
            length,
            checksum,
            ..
        } => Work::Complete(Box::new(Completing {
            bucket,
            key,
            upload,
            listed: Vec::new(),
            native: Some((
                length,
                record::Checksum {
                    algorithm: checksum.algorithm.code(),
                    parts: 0,
                    value: checksum.bytes,
                },
            )),
            preconditions: Preconditions {
                if_match: matched(headers.if_match.as_deref(), true),
                if_none_match: matched(headers.if_none_match.as_deref(), false),
            },
            completion: None,
            waiting: VecDeque::new(),
        })),
        // No part asked: an empty page, the upload not read (05 §4.6).
        Op::ListParts { max: 0, .. } => Work::Draining(Box::new(Reply {
            listing: Some(Listing::Parts(Parts::default())),
            ..Reply::status(Status::Done)
        })),
        Op::ListParts {
            bucket,
            key,
            upload,
            after,
            max,
        } => Work::Read(Box::new(Single::Parts {
            bucket,
            key,
            upload,
            after,
            max: max.min(u32::try_from(MAX_PAGE_PARTS).unwrap_or(u32::MAX)),
        })),
        Op::ListUploads {
            bucket,
            prefix,
            delimiter,
            key_marker,
            id_marker,
            max,
        } => Work::Read(Box::new(Single::Uploads(Query::Uploads {
            bucket,
            prefix,
            delimiter,
            key_marker,
            id_marker,
            max,
        }))),
        Op::List(list) => Work::Read(Box::new(Single::List(list))),
        Op::HeadBucket { bucket } => Work::Read(Box::new(Single::HeadBucket(bucket))),
        Op::ListBuckets { prefix, after, max } => {
            Work::Read(Box::new(Single::Buckets { prefix, after, max }))
        }
    }
}

fn getting(bucket: String, key: String, headers: Headers, head: bool) -> Work {
    Work::Get(Box::new(Getting {
        bucket,
        key,
        headers,
        head,
        get: None,
        out: None,
        written: 0,
        total: 0,
        waiting: VecDeque::new(),
    }))
}

/// What a bucket's create or delete asks next.
enum Next {
    /// A delete's bucket row, whose owner it checks.
    Owner,
    /// Its first command, or its coordinator's next request.
    Request(coordinator::Request),
    /// Nothing: it is over, with this answer.
    Done(Status),
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
    Over(Box<Reply>),
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

/// A completion's driver's answer for what its request came to; none for a read or command
/// refused or answered with another kind.
fn complete_answer(outcome: Outcome) -> Option<complete::Answer> {
    match outcome {
        Outcome::Read(ReadOutcome::Read(read)) => match *read {
            Read::Upload(upload) => Some(complete::Answer::Upload(upload)),
            Read::Parts(Some((parts, _))) => Some(complete::Answer::Parts(parts)),
            // The upload gone while the completion read it: its parts are none, and the
            // driver finds the listed parts missing.
            Read::Parts(None) => Some(complete::Answer::Parts(Vec::new())),
            _ => None,
        },
        Outcome::Command(CommandOutcome::Answered(Answer::File(o))) => {
            Some(complete::Answer::File(o))
        }
        Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => {
            Some(complete::Answer::Name(o))
        }
        _ => None,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(work: &Work) -> Option<Status> {
        match work {
            Work::Draining(reply) => Some(reply.status),
            _ => None,
        }
    }

    /// What a request's head alone refuses, before anything is read or written: a part declared
    /// past S3's 5 GiB, an object past a single PUT's, and a checksum type its algorithm does not
    /// have (05 §3.3, §4.1).
    #[test]
    fn a_request_past_s3s_bounds_is_refused_before_anything_is_read() {
        let part = |length| Op::UploadPart {
            bucket: "b".into(),
            key: "k".into(),
            upload: "0123456789ABC".into(),
            number: 1,
            id: 1,
            length,
            checksum: None,
            md5: None,
            trailing: None,
        };
        assert_eq!(
            refused(&work(part(name::MAX_PART + 1), Vec::new())),
            Some(Status::EntityTooLarge)
        );
        assert_eq!(refused(&work(part(name::MAX_PART), Vec::new())), None);
        let put = Op::Put {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            length: mantle_s3::body::MAX_UPLOAD + 1,
            headers: Headers::default(),
            checksum: None,
            md5: None,
            trailing: None,
        };
        assert_eq!(
            refused(&work(put, Vec::new())),
            Some(Status::EntityTooLarge)
        );
        // A checksum in the fields and a trailer's both.
        let twice = Op::Put {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            length: 1,
            headers: Headers::default(),
            checksum: Some(checksum::Checksum {
                algorithm: checksum::Algorithm::Crc32,
                bytes: vec![0; 4],
            }),
            md5: None,
            trailing: Some(checksum::Algorithm::Crc32),
        };
        assert_eq!(
            refused(&work(twice, Vec::new())),
            Some(Status::InvalidRequest)
        );
        let create = |algorithm, full| Op::CreateUpload {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            checksum: Some((algorithm, full)),
        };
        for (algorithm, full, ok) in [
            (Algorithm::Crc64Nvme, true, true),
            (Algorithm::Crc64Nvme, false, false),
            (Algorithm::Sha256, true, false),
            (Algorithm::Sha256, false, true),
            (Algorithm::Crc32c, true, true),
            (Algorithm::Crc32c, false, true),
        ] {
            let status = refused(&work(create(algorithm, full), Vec::new()));
            assert_eq!(
                status,
                if ok {
                    None
                } else {
                    Some(Status::InvalidRequest)
                },
                "{algorithm:?} full {full}"
            );
        }
    }
}

/// Diagnostic: when each pending ask was first seen waiting, by its kind and number.
static DIAG_WAITS: std::sync::Mutex<Vec<((u8, u64), std::time::Instant)>> =
    std::sync::Mutex::new(Vec::new());

fn diag_key(pending: &Pending) -> (u8, u64) {
    match pending {
        Pending::Command(ask) => (0, *ask),
        Pending::Read(ask) => (1, *ask),
        Pending::Chunk(ask) => (2, *ask),
    }
}

fn diag_work(work: &Work) -> &'static str {
    match work {
        Work::Put(_) => "put",
        Work::Get(_) => "get",
        Work::Command(_) => "command",
        Work::Complete(_) => "complete",
        Work::Read(_) => "read",
        Work::Bucket(_) => "bucket",
        _ => "other",
    }
}

fn diag_seen(key: (u8, u64)) {
    if let Ok(mut waits) = DIAG_WAITS.lock()
        && !waits.iter().any(|(k, _)| *k == key)
    {
        waits.push((key, std::time::Instant::now()));
    }
}

fn diag_taken(key: (u8, u64), outcome: &Outcome, work: &str) {
    let Ok(mut waits) = DIAG_WAITS.lock() else {
        return;
    };
    let Some(at) = waits.iter().position(|(k, _)| *k == key) else {
        return;
    };
    let (_, since) = waits.swap_remove(at);
    let ms = since.elapsed().as_millis();
    let (kind, plain, said) = match outcome {
        Outcome::Command(CommandOutcome::Answered(_)) => ("command", true, String::new()),
        Outcome::Command(other) => ("command", false, format!("{other:?}")),
        Outcome::Read(ReadOutcome::Read(_)) => ("read", true, String::new()),
        Outcome::Read(other) => ("read", false, format!("{other:?}")),
        Outcome::Chunk(ChunkOutcome::Stored | ChunkOutcome::Read(_)) => ("chunk", true, String::new()),
        Outcome::Chunk(other) => ("chunk", false, format!("{other:?}")),
    };
    if ms >= 500 || !plain {
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        eprintln!(
            "diag-wait wall_ms={wall} pid={} work={work} kind={kind} ask={} ms={ms} outcome={said}",
            std::process::id(),
            key.1,
        );
    }
}
