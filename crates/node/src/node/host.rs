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
//! A put's body comes on a bounded channel as its door gives it, a native exchange's by the node's
//! task (node.md §2.9 step 6c), only as fast as the driver takes it, a piece at a time, and a reply
//! goes on a bounded channel its door writes from, a native exchange's by the node's task, a get's
//! bytes taken from its driver only as fast as the door takes them, so what an operation holds is
//! its driver's window of blocks and a piece or two. The operations the host runs at once
//! are bounded (`GatewaySettings::objects`); one past them is answered `SlowDown`.

use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use hyper_rt::compute;
use hyper_rt::slow::Label;
use hyper_rt::sync::{ChannelReceiver, OneshotReceiver, Sender, SyncError};
use hyper_transport::{ExchangeId, PeerId, Refusal};
use mantle_ec::durability::{self, Choice, Rates};
use mantle_gateway::complete::{self, CompleteError, Completion};
use mantle_gateway::get::{self, Get, GetError, Keyring};
use mantle_gateway::layout::Layout;
use mantle_gateway::put::{self, Body, Commit, Expected, Holding, Keys, Put, PutError, Random};
use mantle_meta::apply::Layer;
use mantle_meta::coordinator::{self, Coordinator, Settled};
use mantle_meta::key::{NULL_VERSION, parse_version_id};
use mantle_meta::name::{self, Match, Named, Preconditions};
use mantle_meta::principal;
use mantle_meta::record::{
    self, Bucket, BucketState, Descriptor, How, LISTING, Part, RunMd5, Runs, Upload, Version,
    Versioning, WrappedKey, Wrapper,
};
use mantle_meta::root::{self, Placed};
use mantle_meta::wire::{Answer, Command};
use mantle_meta::{bucket, engine::Engine};
use mantle_s3::checksum::{self, Algorithm};
use mantle_s3::conditional::{self, Current};
use mantle_s3::copy::Directive;
use mantle_s3::range::{self, CopyRangeError, Span};
use mantle_s3::seal::{DataKey, WrappingKey};

use super::keyed::Key;
use super::{ChunkAsk, ChunkOutcome, Node, NodeError, NodeLog, PIECE, shard, take_done};
use crate::ask::{CommandAsk, CommandOutcome, ReadAsk, ReadOutcome, Wait};
use crate::chunk;
use crate::door::{self, Piece};
use crate::labels;
use crate::place;
use crate::read::{Query, Read};
use mantle_protocol::bounds;
use mantle_protocol::classes::Class;
use mantle_protocol::object::{
    self, Deleted, Head, Headers, Issued, List, Listing, MAX_PAGE_PARTS, Metadata, ObjectError, Op,
    PartListed, Parts, Principals, Progress, Reply, Status, Stored, Target,
};

/// What the gateway host is given (docs/design/node.md §4.6, §5).
pub struct GatewaySettings {
    /// The root key's generations, the last the current one, which new files' data keys are
    /// wrapped under (encryption.md §2): a file is opened under the generation its header names.
    pub keys: Vec<(u32, WrappingKey)>,
    /// How long a block or file is held for the write above it (gateway.md §2).
    pub handover_ns: u64,
    /// The blocks a put holds going down, and a get holds being read, at once at the least, the
    /// floor (gateway.md §2–§3): each holds the window the node learned (`crate::window`) where
    /// the operations' bytes hold it.
    pub window: usize,
    /// The most object operations the host runs at once, as many as its operations' bytes hold
    /// at the least one holds: within it the host runs as many as it has learned its knee is
    /// (`crate::admit`), and one past them is answered `SlowDown`.
    pub objects: usize,
    /// The bytes the host's operations hold at once, each charged what it holds at its own size
    /// (`charges`): one whose bytes would pass them waits, or is answered `SlowDown`.
    pub objects_bytes: u64,
    /// The most each range's engine's page cache holds (`engine.cache_bytes`), its want: it
    /// borrows its part of the operations' bytes no running operation holds, and gives it back
    /// before one is charged them ([`lend`]).
    pub cache_bytes: u64,
    pub charges: crate::derive::Charges,
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

/// A native object request parked until the host has room for it (`Node::take_parked`), its body
/// unread: its exchange and asker, its head as decoded, what it will be charged, its client's
/// deadline, and when it is answered `SlowDown` if still parked.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Parked {
    exchange: ExchangeId,
    peer: PeerId,
    head: Option<Head>,
    charge: u64,
    window: usize,
    deadline_ns: Option<u64>,
    until_ns: u64,
}

/// A parked request, found by its exchange as the exchange ends (`Node::job_lost`).
impl super::keyed::Key for Parked {
    type Of = ExchangeId;

    fn key(&self) -> ExchangeId {
        self.exchange
    }
}

/// What the host waits on for an operation: a command, a read, or a chunk it asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Pending {
    Command(CommandAsk),
    Read(ReadAsk),
    Chunk(ChunkAsk),
}

/// What a job asks of the node, one value a lane between shards can carry (node.md §2.9 step
/// 6b), which [`Node::ask`] serves: a range's command or read, asked as a gateway asks one, until
/// the range answers or its budget passes without a leader (`Wait::Request`); or a chunk's put or
/// get on a volume, of the node the volume table names as holding it.
pub(crate) enum JobAsk {
    /// A command of the range of the group.
    Command(u128, Command),
    /// A read of the range of the group.
    Read(u128, Query),
    /// `key`'s whole chunk, `bytes`, with the CRC-32C its writer computed, put on `volume`, which
    /// `at` holds, within the job's `remaining_ns`.
    Put {
        at: PeerId,
        volume: u128,
        key: mantle_chunk::ChunkKey,
        bytes: Bytes,
        crc32c: u32,
        remaining_ns: u64,
    },
    /// `len` bytes from `offset` of `key`'s chunk on `volume`, which `at` holds, within the job's
    /// `remaining_ns`; asked of `at` even while the detectors suspect it where `past_suspicion`
    /// (`get::Request::Chunk`).
    Get {
        at: PeerId,
        volume: u128,
        key: mantle_chunk::ChunkKey,
        span: (u64, u64),
        remaining_ns: u64,
        past_suspicion: bool,
    },
}

/// A native request handed to a driving shard with its fields read (node.md §2.9 step 6e): its
/// operation, not its work, which the shard makes ([`Host::take_handed`]), as a job's drivers need
/// not cross (step 6's check); its door's job half, its asker's principal, its Open's block, its
/// client's deadline, its admission's time, charge and window, and the shard it was placed on.
pub(crate) struct Handover {
    half: Box<Native>,
    principal: String,
    op: Op,
    block: Option<u64>,
    deadline_ns: Option<u64>,
    admitted_ns: Option<u64>,
    charge: u64,
    window: usize,
    driven: Option<usize>,
}

/// An object operation the host runs: the door it came in by, its asker, and where it is.
pub(crate) struct Job {
    door: Door,
    /// Who asks: a native asker's peer id, or the account an S3 request's access key acts for.
    principal: String,
    work: Work,
    /// What the operation waits on, each with the driver's request it answers, if a driver's.
    pending: Vec<(Pending, Option<u64>)>,
    /// When, on the shard's clock, its client stops waiting, where the client said (research/77
    /// D2).
    deadline_ns: Option<u64>,
    /// When its work last moved: begun, an ask of its own answered, a piece of a chunk it reads
    /// or sends.
    moved_ns: u64,
    /// When, on the shard's clock, its asker was last told its work is held (`Job::vouch`); none
    /// before it first was.
    vouched_ns: Option<u64>,
    /// Whether the host's turns pass it by: its last turn left it waiting on its own asks alone
    /// (`Job::rests`), and only what wakes it gives it work again (`Host::wake`).
    resting: bool,
    /// When the host admitted it, on the shard's clock; none for one answered at once, refused or
    /// malformed, which the admission limit does not count (`crate::admit`).
    admitted_ns: Option<u64>,
    /// The bytes it was charged as it was admitted, what it holds at its own size
    /// (`derive::Charges`); none for one not admitted.
    charge: u64,
    /// The blocks a put of it holds going down, or a get of it being read, at once, as the node
    /// learned them (`crate::window`) when it was admitted, a get's once its version was read
    /// (`reading`), and charged for them. A copy's get and put hold their own, fitted once its
    /// source's version is read (`copy_windows`), and this is the floor's.
    window: usize,
    /// The shard, numbered from the node's first, its native request was placed on and is counted
    /// on until it ends (`node::drivers`); none for one not placed: the S3 door's, or one answered
    /// at once.
    driven: Option<usize>,
    /// Its operation's label (`crate::labels::host`), once its operation is known: none while a
    /// native request's fields are read, or for one answered without its operation.
    label: Option<&'static Label>,
}

impl Job {
    /// When, on the shard's clock, the operation's client stops waiting: its own deadline, or the
    /// stock S3 clients' patience past the last time its work moved, whichever comes first
    /// (research/77 D2).
    fn bound_ns(&self) -> u64 {
        let patience = u64::try_from(bounds::STOCK_PATIENCE.as_nanos()).unwrap_or(u64::MAX);
        let quiet = self.moved_ns.saturating_add(patience);
        self.deadline_ns
            .map_or(quiet, |deadline| deadline.min(quiet))
    }

    /// Whether its turn has nothing to do until an event that wakes it comes (`Host::wake`,
    /// node.md §2.9 step 6d): one of its own asks answered, room offered it, its bound, its put's
    /// renewal due, its S3 connection naming it (`door::Mark`) as it sends a piece of its body,
    /// takes an answer of its reply or lets go, or the node's task naming its native job
    /// (`Call`) as it gives a piece of its body or lets go of it, takes or writes whole an answer
    /// of its reply, or lets go of its reply, or waking one reading its fields as more of its
    /// body arrives (`Host::arrived`). Its turn would feed it nothing, ask nothing and
    /// answer nothing where it holds no request for want of the node's room and no chunk for want
    /// of a volume's, and:
    ///
    /// - a put or a get reads its bucket's row or its version, or its get has no bytes to give
    ///   while its reads are out;
    /// - a put not over whose driver takes none of its body now waits on its asks;
    /// - a put whose driver wants its body found its door's channel empty, or a get or its last
    ///   answers found no room in its door's reply, a native one's until the node's task wrote
    ///   the last whole: the door names it as more comes or room frees;
    /// - any other kind has an ask of its own out and nothing more to ask or feed until one is
    ///   answered: a single read, a command, a bucket's or an operator's request, or a copy
    ///   reading a row, version or mark, at once; a batch delete with none refused for the
    ///   node's room; a completion or a running copy with no request held for it and no outcome
    ///   to answer, a copy's get holding nothing its put takes now. None of these reads its door.
    /// - a native job reading its fields read what had arrived of them until a read took none
    ///   (`Node::read_fields`), the transport's `EAGAIN`: the transport says the next arrival
    ///   (`Event::BodyReady`), and the node's task wakes the job as it does (`Host::arrived`).
    ///
    /// A job whose door names it not, made with no mark or call, is turned at every step.
    fn rests(&self) -> bool {
        let asking = !self.pending.is_empty();
        // Whether the node's task names it as its door moves, holding its reply still.
        let called = matches!(&self.door, Door::Native(_, native) if native.called());
        // What its door gives it next comes named: whether its body's channel was found empty,
        // nothing of it ended, and whether its reply has no room for the next answer, a native
        // one's until the node's task wrote the last whole. One whose connection or reply was let
        // go waits on its door no more: its channels close as it goes.
        let (dry, roomless) = match &self.door {
            Door::Local(local) if local.mark.as_ref().is_some_and(|mark| !mark.gone()) => (
                local
                    .body
                    .as_ref()
                    .is_some_and(|body| body.dry && body.ended.is_none()),
                !local.pending.is_empty(),
            ),
            Door::Native(_, native) if native.called() => (
                native.body.dry && native.body.ended.is_none(),
                !native.written(),
            ),
            Door::Local(_) | Door::Native(..) => (false, false),
        };
        match &self.work {
            Work::Put(putting) => match putting.put.as_ref() {
                None => asking,
                Some(put) if put.outcome().is_some() || put.waits_for_room() => false,
                Some(_) if !putting.waiting.is_empty() => false,
                Some(put) if !put.wants_body() => asking,
                // What it carried over of its last piece is fed before any more is read.
                Some(_) if !putting.carry.is_empty() => false,
                Some(_) => dry,
            },
            Work::Get(getting) => match getting.get.as_ref() {
                None => asking,
                Some(get) if get.waits_for_room() || !getting.waiting.is_empty() => false,
                Some(get) => roomless || (asking && !get.ready() && get.outcome().is_none()),
            },
            Work::Answering(_) => roomless,
            // One read, one command, a bucket's coordinator's next request, an operator's
            // request, and a copy's row, version or mark: its step asks nothing while an ask of
            // its own is out.
            Work::Read(_) | Work::Command(_) | Work::Bucket(_) | Work::Admin(_) => asking,
            Work::Copy(copying) if copying.stage != CopyStage::Run => asking,
            // Its deletes out to its share of the node's commands, or every one asked: only an
            // answer lets it ask the next.
            Work::Deletes(deleting) => asking && !deleting.held,
            // Its driver asks the next request as an answer moves it, none held for want of the
            // node's room.
            Work::Complete(completing) => {
                asking
                    && completing.waiting.is_empty()
                    && completing
                        .completion
                        .as_ref()
                        .is_none_or(|completion| completion.outcome().is_none())
            }
            // Its get and its put each wait on their asks, as a get and a put do: the get holds no
            // plaintext the put could take, or the put takes none now, and neither driver has
            // a request held for want of room or an outcome to answer.
            Work::Copy(copying) => {
                let (Some(get), Some(put)) = (copying.get.as_ref(), copying.put.as_ref()) else {
                    return false;
                };
                let fed = match &copying.carry {
                    Some((bytes, took)) => bytes.len() > *took && !put.wants_body(),
                    None => !get.ready(),
                };
                let got = match get.outcome() {
                    None => true,
                    Some(Ok(())) => copying.ended,
                    Some(Err(_)) => false,
                };
                asking
                    && fed
                    && got
                    && put.outcome().is_none()
                    && !put.waits_for_room()
                    && !get.waits_for_room()
                    && copying.get_waiting.is_empty()
                    && copying.put_waiting.is_empty()
            }
            // Its step leaves it reading its fields only where its last read took none of them:
            // each read of a body arriving lets the transport say the next arrival
            // (hyper-transport's `ready_sent`), and its fields are the body's first bytes
            // (`object::Head::decode`), so they are arriving while any is owed.
            Work::Fields(..) => called,
            Work::Handing(_) | Work::Draining(_) => false,
        }
    }

    /// Whether its work is held, by a peer or a device: an ask of its own out, or a chunk waiting
    /// for room at a volume or in the transport toward it.
    fn held(&self) -> bool {
        !self.pending.is_empty() || self.waits_for_room()
    }

    /// Whether a chunk its put sends or its get reads waits for room at a volume or in the
    /// transport toward it.
    fn waits_for_room(&self) -> bool {
        match &self.work {
            Work::Put(putting) => putting.put.as_ref().is_some_and(Put::waits_for_room),
            Work::Get(getting) => getting.get.as_ref().is_some_and(Get::waits_for_room),
            Work::Copy(copying) => {
                copying.put.as_ref().is_some_and(Put::waits_for_room)
                    || copying.get.as_ref().is_some_and(Get::waits_for_room)
            }
            _ => false,
        }
    }

    /// What to do while its work is held: vouch within its patience, answer a refusal past it,
    /// or stop vouching if its reply already began. A get's driver is made with its reply head
    /// (`current_read`); before that, it can still be answered `SlowDown`.
    fn bounded(&self, now: u64) -> Bounded {
        if now < self.bound_ns() {
            Bounded::Held
        } else if matches!(&self.work, Work::Get(getting) if getting.get.is_some() && getting.held.is_none())
        {
            Bounded::Unvouched
        } else {
            Bounded::SlowDown
        }
    }

    /// Its next bound or working beat, while its own work is held. A get whose reply began
    /// and whose patience passed is vouched for no more and needs no beat, but one whose chunk
    /// read still waits for room is due at once, its driver ended then (`Host::bound_jobs`); a get
    /// still reading its version must wake at the bound even through the local S3 door. A put or
    /// a get whose chunk waits for room is due at the beat on either door: no answer of its own
    /// comes to turn the node when a peer's volume frees room, so the host offers it room again
    /// then (`Host::step`). A native one is due at its next vouch (`Job::vouch`): at once where it
    /// was never vouched for or its work moved since it last was, else half a beat after it last
    /// was.
    fn due_ns(&self, now: u64, beat: u64) -> Option<u64> {
        if self.bounded(now) == Bounded::Unvouched {
            return if self.waits_for_room() {
                Some(now)
            } else {
                None
            };
        }
        let vouch = match (&self.door, self.vouched_ns) {
            (Door::Local(_), _) => None,
            (Door::Native(..), Some(vouched)) if self.moved_ns <= vouched => {
                Some(vouched.saturating_add(vouch_every(beat)))
            }
            (Door::Native(..), _) => Some(now),
        };
        let room = self.waits_for_room().then(|| now.saturating_add(beat));
        [Some(self.bound_ns()), vouch, room]
            .into_iter()
            .flatten()
            .min()
    }

    /// When, on the shard's clock, the host's bound pass next has work for it
    /// (`Host::bound_jobs`), none where it has none before its state moves, which keys it again
    /// (`Host::rekey`): the pass reads the jobs due alone, so this is every time it acts on one.
    /// A held one's is its bound or its beat (`Job::due_ns`); one at work holding nothing out, on
    /// its native door, is due half a beat after it was last told, or admitted, where its own
    /// bytes moved since (`Job::moving`). Each is the first time the pass, which read every job
    /// at every turn, would act: no word to an asker comes later than it did. A key set at a turn
    /// is never later than one set at a later turn on the job's state as it was, which a resting
    /// job keeps.
    fn beat_ns(&self, now: u64, beat: u64) -> Option<u64> {
        if !self.working() {
            None
        } else if self.held() {
            self.due_ns(now, beat)
        } else if let Door::Native(..) = self.door {
            let since = self.vouched_ns.or(self.admitted_ns)?;
            (self.moved_ns > since).then(|| since.saturating_add(vouch_every(beat)))
        } else {
            None
        }
    }

    /// When, on the wall clock, its put next renews what it holds (`Put::due_ns`): a put's or a
    /// copy's, by its put's state alone.
    fn renewal_ns(&self) -> Option<u64> {
        match &self.work {
            Work::Put(putting) => putting.put.as_ref()?.due_ns(),
            Work::Copy(copying) => copying.put.as_ref()?.due_ns(),
            _ => None,
        }
    }

    /// Tells its asker its work is held, through its native door, at most twice a beat
    /// (`vouch_every`) unless its work moved since it last did: a vouch reaches the asker in the next note on its connection,
    /// notes at most a beat apart, and its asker judges the exchange by it (hyper-transport's
    /// `Endpoint::working`; research/77 D1, said as work moves, and D2, each beat while held). One
    /// told at every turn, thousands a beat on a loaded node, said nothing more and cost the most
    /// (macOS `sample` of a node under the native GET bench's 2,048 clients, 2026-11-19: 23% of its
    /// first shard in the transport's `tally::Table::settle`, 88% of that under
    /// `Endpoint::working`). The S3 door's asker is told nothing: its connection is the listener's.
    fn vouch(&mut self, now: u64, beat: u64) {
        let Door::Native(_, native) = &self.door else {
            return;
        };
        let every = vouch_every(beat);
        let due = self
            .vouched_ns
            .is_none_or(|vouched| self.moved_ns > vouched || now >= vouched.saturating_add(every));
        if due {
            native.held();
            self.vouched_ns = Some(now);
        }
    }

    /// Tells its asker its work moves, through its native door, where its own bytes moved since it
    /// was last told (its body taken or its reply given, `Host::turn_job`), at most twice a beat
    /// (`vouch_every`) from then, or from its admission before it first was: a job holding nothing
    /// of its own out says its work moves as it moves (research/77 D1), as a held one says each
    /// beat that it is held (`Job::vouch`), and nothing while it stands still. Its asker counts
    /// each window from the note that begins it (hyper-transport's `Carry::noted`). Holding
    /// nothing, a put taking its body said nothing: CI's put of 20 MiB to a node on
    /// ubuntu-24.04-arm (2026-11-19) was named once, 4.76 ms after it came, in the 6,119 ms its
    /// node took 8,333,704 bytes of its body, and its client's judgement rested on that note. One
    /// done within half a beat of its admission is told nothing.
    fn moving(&mut self, now: u64, beat: u64) {
        let Door::Native(_, native) = &self.door else {
            return;
        };
        let Some(since) = self.vouched_ns.or(self.admitted_ns) else {
            return;
        };
        if self.moved_ns > since && now >= since.saturating_add(vouch_every(beat)) {
            native.held();
            self.vouched_ns = Some(now);
        }
    }

    /// One of its own asks answered at `now`: its work moved, unless a chunk of it was refused for
    /// want of room ([`Taken::Room`]), which moves nothing; else a put or a get asked again at each
    /// beat and refused again would never reach its client's patience (research/77 D2).
    fn answered(&mut self, outcome: &Outcome, now: u64) {
        if !matches!(outcome, Outcome::Chunk(chunk) if taken(chunk) == Taken::Room) {
            self.moved_ns = now;
        }
    }

    /// Whether the operation still works toward its reply, rather than giving one or reading the
    /// fields that say what it is.
    fn working(&self) -> bool {
        !matches!(
            self.work,
            Work::Fields(..) | Work::Handing(_) | Work::Draining(_) | Work::Answering(_)
        )
    }
}

/// The object operations a task runs (node.md §2.9 step 6d): its jobs and what they hold, the
/// bytes their work moved, the native doors kept for its next requests ([`Doors`]), and the
/// gateway's settings its jobs run by, shared with the node. Its steps are served by the node
/// through one interface ([`Serve`]), which lends it out for each turn: the node keeps admission,
/// parking and the windows it learned, one authority for its bottleneck (§2.5), the asks and their
/// outcomes, and the exchanges, each request's fields read and its reply written by its own task.
/// A host taken for a lend leaves its default in its place, which holds nothing and allocates
/// nothing (`Vec::new`).
#[derive(Default)]
pub(crate) struct Host {
    jobs: Vec<Job>,
    /// Bytes of its jobs' bodies taken and of their replies given, as each job counted them since
    /// the host was made: summed with the node's own where `Node::moved` is read.
    moved: u64,
    doors: Doors,
    /// The gateway's settings, one `Arc` made with the node and shared with it, none where the
    /// node hosts no objects.
    gateway: Option<std::sync::Arc<GatewaySettings>>,
    /// When, on the shard's clock, the host last offered its jobs room, or last had none waiting
    /// for it: a put or a get whose chunk waits for room is offered it again a beat after
    /// (`Host::step`).
    room_offered_ns: u64,
    /// Its jobs whose chunk waits for room (`Job::waits_for_room`), each keyed by when, on the
    /// shard's clock, it began to wait: room freed is offered to these alone, the first come
    /// first (`Host::offer_room`), where a walk of every job looked for them. Kept as each job is
    /// keyed (`Host::rekey`). Bound: one a job.
    rooms: Earliest,
    /// Its jobs awake, each keyed by when it was woken, counted (`woken`): a turn steps these
    /// alone, the first woken first, each once (`Host::step`), where a walk of every job passed
    /// each resting one by. A job is here exactly where it does not rest (`Job::resting`), put
    /// here as it is woken (`Host::wake`) and taken out as its turn leaves it resting
    /// (`Host::rest`). Bound: one a job.
    ready: Earliest,
    /// Its jobs whose fields were read for a driving shard (`Work::Handing`), each keyed by when,
    /// on the shard's clock, it was first keyed so: the node's task hands these alone, the first
    /// first (`Host::handing`, `Node::hand_off`), where a pass of every job looked for them at
    /// each turn. Kept as each job is keyed (`Host::rekey`). Bound: one a job.
    handovers: Earliest,
    /// Wakes counted, the key of the next job woken (`Host::wake`): from a `u64` count no node's
    /// wakes wrap.
    woken: u64,
    /// The slots a turn takes due from its heaps, its beats' and its renewals', and the waiters it
    /// offers room in their order, kept from turn to turn so a turn makes none: at most one a job.
    due: Vec<usize>,
    offered: Vec<(u64, usize)>,
    /// Its jobs' slots by the native exchange each came in on (`Door::exchange`), one exchange a
    /// job, each request's: a reply's job found by its exchange (`Node::write_replies`), not by a
    /// search of every job. Kept as each job is pushed and swap-removed (`Host::push`,
    /// `Host::remove`).
    exchanges: Slots<ExchangeId>,
    /// Its jobs' slots by each ask a job waits on (`Job::pending`), one job an ask: the job a piece
    /// of a chunk moved for found by its ask (`Host::chunk_moved`), not by a search of every job's
    /// asks. Kept as each ask is waited on (`Host::wait_on`), answered (`take_answers`) or let go
    /// (`reply_job`), and as each job is pushed and swap-removed.
    asks: Slots<Pending>,
    /// Its S3 jobs' slots by their requests' names (`door::Mark`), one a job: the job its
    /// connection named found in one probe (`Host::take_marks`), kept as `exchanges` is.
    named: Slots<u64>,
    /// Where the node's task names its native jobs as it moves their doors ([`Calls`]), taken at
    /// each step before any job is stepped (`Host::take_calls`); none for a host no node's task
    /// names jobs to, whose jobs never rest on their doors.
    calls: Option<Calls>,
    /// Its native jobs' slots by the names their calls name them by (`Native::name`), one a job:
    /// the job named found in one probe, kept as `named` is.
    called: Slots<u64>,
    /// The name the next native request its node's task takes is given (`Native::name`): one a
    /// request, from a `u64` count no node's requests wrap.
    next_name: u64,
    /// The slots of the jobs a turn's answers are for (`Host::step`), kept from turn to turn so a
    /// turn makes none: at most the asks answered at once, each a job's, which `asks` bounds.
    answered: Vec<usize>,
    /// Its held jobs' next beats or bounds on the shard's clock (`Job::due_ns`), and its puts'
    /// next renewals on the wall clock (`Job::renewal_ns`), each job's keyed from its state as
    /// its turn ends (`Host::rekey`), the earliest of each read in one step by `Host::due`.
    beats: Earliest,
    renewals: Earliest,
}

/// Job slots by a key each job holds, kept in step with the jobs' vector as a job is pushed and
/// swap-removed: a job found by its key in one probe of a hash table rather than a search of every
/// job, thousands on a loaded node's task. Bound: the keys the jobs hold, each once.
struct Slots<K> {
    of: std::collections::HashMap<K, usize>,
}

impl<K> Default for Slots<K> {
    /// None, which allocates nothing (`HashMap::new`): a host lent out leaves its default.
    fn default() -> Self {
        Self {
            of: std::collections::HashMap::new(),
        }
    }
}

impl<K: std::hash::Hash + Eq> Slots<K> {
    /// The slot of the job holding `key`.
    fn get(&self, key: &K) -> Option<usize> {
        #[cfg(test)]
        tests::PROBES.with(|probes| probes.set(probes.get().saturating_add(1)));
        self.of.get(key).copied()
    }

    /// The keys it holds.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.of.len()
    }

    /// A job holding `keys` is at `at`: pushed there, or moved there from the end.
    fn hold(&mut self, keys: impl IntoIterator<Item = K>, at: usize) {
        for key in keys {
            self.of.insert(key, at);
        }
    }

    /// A job holding `keys` let go of them, swap-removed.
    fn release(&mut self, keys: impl IntoIterator<Item = K>) {
        for key in keys {
            self.of.remove(&key);
        }
    }
}

/// Job slots' keys, the least first: a binary min-heap each of whose entries names its job's slot,
/// and each slot its entry's place (a priority queue with handles: Cormen, Leiserson, Rivest and
/// Stein, Introduction to Algorithms, 3rd ed., 2009, §6.5), kept in step with the jobs' vector as
/// a job is pushed and swap-removed. The least is read in one step where a search read every
/// job's; a key set or let go moves its entry a level a step, at most the heap's depth, the log2 of
/// its entries. Bound: one entry a job.
#[derive(Default)]
struct Earliest {
    /// Each key with its job's slot, none less than its parent's (the entry at `(place - 1) / 2`).
    heap: Vec<(u64, usize)>,
    /// Each job slot's place in `heap`, none for a job with no key: one a job.
    place: Vec<Option<usize>>,
}

impl Earliest {
    /// The least key.
    fn first(&self) -> Option<u64> {
        self.heap.first().map(|(key, _)| *key)
    }

    /// The least key with its slot.
    fn least(&self) -> Option<(u64, usize)> {
        self.heap.first().copied()
    }

    /// Slot `at`'s key.
    fn key(&self, at: usize) -> Option<u64> {
        let place = self.place.get(at).copied().flatten()?;
        self.heap.get(place).map(|(key, _)| *key)
    }

    /// Whether no slot is keyed.
    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Each key with its slot, in the heap's order: the slots keyed, and only those.
    fn entries(&self) -> &[(u64, usize)] {
        &self.heap
    }

    /// The slots keyed `until` or before, into `due`, cleared first: the heap read from its root,
    /// level by level, each entry due naming its children, and no entry past `until` naming any,
    /// as none of its children's keys is less than its own. A pass reads the entries due and at
    /// most their children, twice and once more as many as are due, not every entry: a host's
    /// turn takes the few jobs due among thousands kept. `due` holds the heap's places as they
    /// are read, then their slots.
    fn due(&self, until: u64, due: &mut Vec<usize>) {
        due.clear();
        if self.first().is_none_or(|first| first > until) {
            return;
        }
        due.push(0);
        let mut read = 0;
        // Each place is named once, by its parent: at most every entry's.
        for _ in 0..self.heap.len() {
            let Some(&place) = due.get(read) else {
                break;
            };
            read = read.saturating_add(1);
            let left = place.checked_mul(2).and_then(|p| p.checked_add(1));
            let right = left.and_then(|left| left.checked_add(1));
            for child in [left, right].into_iter().flatten() {
                if self.heap.get(child).is_some_and(|(key, _)| *key <= until) {
                    due.push(child);
                }
            }
        }
        for place in due.iter_mut() {
            if let Some((_, slot)) = self.heap.get(*place) {
                *place = *slot;
            }
        }
    }

    /// A job pushed, holding no key yet.
    fn grow(&mut self) {
        self.place.push(None);
    }

    /// Slot `at`'s key set to `key`, or let go where none.
    fn set(&mut self, at: usize, key: Option<u64>) {
        let Some(&held) = self.place.get(at) else {
            return;
        };
        match (held, key) {
            (Some(place), Some(key)) => {
                // A key kept as it was, a held job's from turn to turn, moves nothing.
                match self.heap.get_mut(place) {
                    Some(entry) if entry.0 != key => entry.0 = key,
                    _ => return,
                }
                self.settle(place);
            }
            (Some(place), None) => self.take(place),
            (None, Some(key)) => {
                let place = self.heap.len();
                self.heap.push((key, at));
                if let Some(held) = self.place.get_mut(at) {
                    *held = Some(place);
                }
                self.settle(place);
            }
            (None, None) => {}
        }
    }

    /// Slot `at` let go as its job is swap-removed: its key taken out, and the last slot's, the job
    /// moved into `at`, named there.
    fn swap_remove(&mut self, at: usize) {
        let Some(&held) = self.place.get(at) else {
            return;
        };
        if let Some(place) = held {
            self.take(place);
        }
        let last = self.place.pop().flatten();
        if let Some(held) = self.place.get_mut(at) {
            *held = last;
            if let Some(entry) = last.and_then(|place| self.heap.get_mut(place)) {
                entry.1 = at;
            }
        }
    }

    /// The entry at `place` taken out: the heap's last put in its place and settled there.
    fn take(&mut self, place: usize) {
        let Some(&(_, slot)) = self.heap.get(place) else {
            return;
        };
        if let Some(held) = self.place.get_mut(slot) {
            *held = None;
        }
        let Some(last) = self.heap.pop() else {
            return;
        };
        if let Some(entry) = self.heap.get_mut(place) {
            *entry = last;
            if let Some(held) = self.place.get_mut(last.1) {
                *held = Some(place);
            }
            self.settle(place);
        }
    }

    /// The entry at `place` moved up past each parent whose key is greater, then down past the
    /// lesser child whose key is less, until no key is less than its parent's: a level a step, so
    /// at most the heap's depth, fewer steps than a `usize` has bits.
    fn settle(&mut self, mut place: usize) {
        for _ in 0..usize::BITS {
            let Some(parent) = place.checked_sub(1).and_then(|p| p.checked_div(2)) else {
                break;
            };
            if !self.less(place, parent) {
                break;
            }
            self.exchange(place, parent);
            place = parent;
        }
        for _ in 0..usize::BITS {
            let left = place.checked_mul(2).and_then(|p| p.checked_add(1));
            let right = left.and_then(|left| left.checked_add(1));
            let mut least = place;
            for child in [left, right].into_iter().flatten() {
                if self.less(child, least) {
                    least = child;
                }
            }
            if least == place {
                break;
            }
            self.exchange(place, least);
            place = least;
        }
    }

    /// Whether the key at `a` is less than the key at `b`, both held.
    fn less(&self, a: usize, b: usize) -> bool {
        #[cfg(test)]
        tests::COMPARED_KEYS.with(|compared| compared.set(compared.get().saturating_add(1)));
        match (self.heap.get(a), self.heap.get(b)) {
            (Some((a, _)), Some((b, _))) => a < b,
            _ => false,
        }
    }

    /// The entries at `a` and `b` swapped, each slot naming its entry's new place.
    fn exchange(&mut self, a: usize, b: usize) {
        let Ok([first, second]) = self.heap.get_disjoint_mut([a, b]) else {
            return;
        };
        std::mem::swap(first, second);
        let (at_a, at_b) = (first.1, second.1);
        if let Some(held) = self.place.get_mut(at_a) {
            *held = Some(a);
        }
        if let Some(held) = self.place.get_mut(at_b) {
            *held = Some(b);
        }
    }
}

/// What the host asks of whoever lends it a turn, the node's task now and a driving shard's task
/// at 6e (node.md §2.9 step 6d): each ask of a job and its outcome (6b), the volume table and the
/// ranges' groups, whether the node holds room for a command, the wall clock, a local door's
/// pacing, the put and get windows' samples, the windows learned and room for a job's charge to
/// grow, and each job's end with its charge, its shard and the asks it let go. A native job's fields are read by the node's task (`read_fields`) until 6f
/// reads them before the job is made.
pub(crate) trait Serve {
    /// Asks what a job asks: what it waits on, whose outcome [`Serve::outcome`] gives. Refused
    /// `NodeError::Asks` where the node holds as many of its kind as it keeps, which the job asks
    /// again at its next turn.
    fn ask(&mut self, ask: JobAsk) -> Result<Pending, NodeError>;
    /// The outcome of what a job asked, once answered, taken.
    fn outcome(&mut self, pending: Pending) -> Option<Outcome>;
    /// The asks answered whose outcomes are not yet taken, a job's or another's: the host's next
    /// turn is due at once where one is a job's ([`Host::due`]).
    fn answers(&self) -> impl Iterator<Item = Pending>;
    /// Asks a job let go of, their job ended or replied before they were answered: each outcome
    /// dropped as it comes, as one no job takes would hold one of the node's asks for good.
    fn abandon(&mut self, asks: impl IntoIterator<Item = Pending>);
    /// The volume table puts are placed by, as the node last read it.
    fn table(&mut self) -> Result<&Table, NodeError>;
    /// The group of the range of `layer` the node hosts.
    fn group_of(&self, layer: Layer) -> Option<u128>;
    /// The ranges the node hosts, which a bucket's coordinator bounds its reads by.
    fn ranges(&self) -> usize;
    /// Whether the node holds room for one more command (`command_asks`).
    fn command_room(&self) -> bool;
    /// Whether the node holds room for one more ask of `ask`'s kind (`command_asks`, `read_asks`,
    /// `chunk_asks`), checked before the ask's value is given up: a driving shard's ask the node
    /// has no room for is held by the node's task, not answered `Asks` (node.md §2.9 step 6e).
    fn room(&self, ask: &JobAsk) -> bool;
    /// The commands the node holds at once (`command_asks`), which a DeleteObjects shares.
    fn command_asks(&self) -> usize;
    /// The wall clock's time, which a put's and a completion's records carry.
    fn wall_ns(&self) -> u64;
    /// Whether room at a volume or in the transport toward it was freed since the last turn, so
    /// each chunk a put or a get holds for want of it is asked again (research/77 D2).
    fn freed(&mut self) -> bool;
    /// Bytes the node's task moved since it was made, a native job's fields among them.
    fn moved(&self) -> u64;
    /// A native job's fields read from its exchange, a piece at a time, and once whole the work
    /// they name; whether it is over.
    fn read_fields(&mut self, job: &mut Job) -> Result<bool, NodeError>;
    /// A piece of an S3 body taken as `paced` says: a sample of the S3 door's depth.
    fn took(&mut self, paced: door::Paced);
    /// A put's blocks recorded in `ns`: a sample of the put window (`Window::took`).
    fn committed(&mut self, ns: u64);
    /// A put's body of `bytes` came in `ns`: a sample of the put window (`Window::paced`).
    fn filled(&mut self, bytes: u64, ns: u64);
    /// A get's block read in `ns`, from its row asked to its plaintext opened: a sample of the get
    /// window (`Window::took`).
    fn read(&mut self, ns: u64);
    /// What the node learned of its puts' and its gets' blocks and bodies, the windows each holds
    /// (`crate::window`), which a get's and a copy's are fitted to once their sizes are known.
    fn windows(&self) -> crate::window::Windows;
    /// Whether the operations' bytes hold `more` past a running job's charge beside the others'
    /// (`admit::Admission::holds`).
    fn holds(&self, more: u64) -> bool;
    /// A running job's charge moved as its windows were fitted: `more` bytes taken past it, which
    /// [`Serve::holds`] said the operations' bytes hold, the caches giving back first what of them
    /// they hold (`Node::charge_more`), and `less` given back.
    fn recharge(&mut self, more: u64, less: u64);
    /// A job ended: admitted at `admitted_ns` and charged `charge`, placed on shard `driven`,
    /// with what it still waited on let go, and its native door's half (`half`), told already
    /// that its job let go: what the host keeps of it, back. The node's task holds a native door's
    /// other half, so its host keeps the half, in its doors for the next request; a driving
    /// shard's host keeps none, the half riding the job's end to the node's task (node.md §2.9
    /// step 6e), as a half its host kept would wait there with no bound for a half it never sees.
    fn job_ended(
        &mut self,
        admitted_ns: Option<u64>,
        charge: u64,
        driven: Option<usize>,
        asks: impl IntoIterator<Item = Pending>,
        half: Option<Box<Native>>,
    ) -> Option<Box<Native>>;
}

impl Host {
    /// A host with no jobs, keeping at most `doors` native doors for its next requests: one an
    /// operation it runs at once, within the exchanges the transport holds; its jobs run by
    /// `gateway`.
    pub(super) fn new(doors: usize, gateway: Option<std::sync::Arc<GatewaySettings>>) -> Self {
        Self {
            jobs: Vec::new(),
            moved: 0,
            doors: Doors::new(doors),
            gateway,
            room_offered_ns: 0,
            rooms: Earliest::default(),
            ready: Earliest::default(),
            handovers: Earliest::default(),
            woken: 0,
            due: Vec::new(),
            offered: Vec::new(),
            exchanges: Slots::default(),
            asks: Slots::default(),
            named: Slots::default(),
            calls: None,
            called: Slots::default(),
            next_name: 0,
            answered: Vec::new(),
            beats: Earliest::default(),
            renewals: Earliest::default(),
        }
    }

    /// Runs `job` from `now`, the shard's clock: last of its jobs, its exchange's and its asks'
    /// slots kept, and its keys.
    fn push(&mut self, job: Job, now: u64) {
        let at = self.jobs.len();
        self.exchanges.hold(job.door.exchange(), at);
        self.named.hold(job.door.named(), at);
        self.called.hold(job.door.called(), at);
        self.asks
            .hold(job.pending.iter().map(|(pending, _)| *pending), at);
        let resting = job.resting;
        self.jobs.push(job);
        self.beats.grow();
        self.renewals.grow();
        self.rooms.grow();
        self.ready.grow();
        self.handovers.grow();
        if !resting {
            self.wake(at);
        }
        self.rekey(at, now);
        #[cfg(test)]
        self.audit();
    }

    /// Its job at `at` taken out, the last moved into its slot, both jobs' exchanges' and asks'
    /// slots and their keys kept.
    fn remove(&mut self, at: usize) -> Option<Job> {
        if at >= self.jobs.len() {
            return None;
        }
        let job = self.jobs.swap_remove(at);
        self.exchanges.release(job.door.exchange());
        self.named.release(job.door.named());
        self.called.release(job.door.called());
        self.asks
            .release(job.pending.iter().map(|(pending, _)| *pending));
        self.beats.swap_remove(at);
        self.renewals.swap_remove(at);
        self.rooms.swap_remove(at);
        self.ready.swap_remove(at);
        self.handovers.swap_remove(at);
        if let Some(moved) = self.jobs.get(at) {
            self.exchanges.hold(moved.door.exchange(), at);
            self.named.hold(moved.door.named(), at);
            self.called.hold(moved.door.called(), at);
            self.asks
                .hold(moved.pending.iter().map(|(pending, _)| *pending), at);
        }
        #[cfg(test)]
        self.audit();
        Some(job)
    }

    /// Job `at` waits on `pending`, the answer to its driver's `request` where a driver's: kept
    /// with its asks, and its slot by the ask. Every ask a job waits on is kept here, so its slot
    /// is never missed; nothing is kept where no job is at `at`.
    fn wait_on(&mut self, at: usize, pending: Pending, request: Option<u64>) {
        let Some(job) = self.jobs.get_mut(at) else {
            return;
        };
        job.pending.push((pending, request));
        self.asks.hold(Some(pending), at);
        #[cfg(test)]
        self.audit();
    }

    /// Job `at` woken, so the host's next turn steps it: each event that can give a resting job
    /// work wakes it as it comes, an answer to one of its asks (`answer_job`), a reply given it
    /// (`reply_job`), room offered it (`offer_room`), its put's renewal coming due
    /// (`Host::step`) and more of its fields arriving (`Host::arrived`); nothing else moves a job
    /// that rests (`Job::rests`).
    ///
    /// A job woken is kept with the jobs awake (`ready`), keyed by its wake, and keeps its key
    /// where it was awake already: the first woken is stepped first.
    fn wake(&mut self, at: usize) {
        let Some(job) = self.jobs.get_mut(at) else {
            return;
        };
        job.resting = false;
        if self.ready.key(at).is_none() {
            self.ready.set(at, Some(self.woken));
            self.woken = self.woken.wrapping_add(1);
        }
    }

    /// More of the body of job `at`'s request arrived (`Event::BodyReady`, taken by the node's
    /// task before the host's step): the job woken where it reads its fields, which it does on
    /// the node's own host alone, before any is handed to a driving shard. Any other job's body
    /// comes through its feeding, which names it as it gives a piece (`Feeding::send`).
    pub(super) fn arrived(&mut self, at: usize) {
        if matches!(
            self.jobs.get(at),
            Some(Job {
                work: Work::Fields(..),
                ..
            })
        ) {
            self.wake(at);
        }
    }

    /// Job `at`'s turn ended: it rests where it waits on its own asks and its door alone
    /// (`Job::rests`), taken out of the jobs awake, else it is stepped again at the next turn,
    /// last of those awake, as every job that does not rest is.
    fn rest(&mut self, at: usize) {
        let Some(job) = self.jobs.get_mut(at) else {
            return;
        };
        job.resting = job.rests();
        if job.resting {
            self.ready.set(at, None);
        } else {
            self.ready.set(at, Some(self.woken));
            self.woken = self.woken.wrapping_add(1);
        }
    }

    /// The S3 requests their connections named since the last turn (`door::Mark`), each one's job
    /// woken and its name let go, so what its connection does next names it again: at most the
    /// marks' room a turn, as `Node::take_marks` takes the native ones. Where a name found no
    /// room, every S3 job is woken, its name let go. A name whose job ended finds none.
    pub(super) fn take_marks(&mut self, doorway: &mut door::Doorway) {
        if doorway.missed() {
            for at in 0..self.jobs.len() {
                if let Some(Job {
                    door: Door::Local(local),
                    ..
                }) = self.jobs.get(at)
                {
                    if let Some(mark) = &local.mark {
                        mark.taken();
                    }
                    self.wake(at);
                }
            }
        }
        for _ in 0..doorway.marks_room() {
            let Some(id) = doorway.marked() else {
                break;
            };
            let Some(at) = self.named.get(&id) else {
                continue;
            };
            if let Some(Job {
                door: Door::Local(local),
                ..
            }) = self.jobs.get(at)
                && let Some(mark) = &local.mark
            {
                mark.taken();
            }
            self.wake(at);
        }
    }

    /// Takes `calls` for its native jobs ([`Calls`]): the node's task names each job on them as it
    /// moves its door.
    pub(crate) fn calling(&mut self, calls: Option<Calls>) {
        self.calls = calls;
    }

    /// The caller that names its jobs to this host, where it has calls.
    pub(super) fn caller(&self) -> Option<std::sync::Arc<Caller>> {
        self.calls.as_ref().map(Calls::caller)
    }

    /// A name for the next native request the node's task takes (`Native::name`).
    fn name_next(&mut self) -> u64 {
        let name = self.next_name;
        self.next_name = self.next_name.wrapping_add(1);
        name
    }

    /// The native jobs the node's task named since the last step ([`Calls`]), each one's word let
    /// go, then the job woken: the step that follows sees what moved its door before the word was
    /// let go, and what moves it after names it again, so no move is lost and at most one step
    /// repeats. At most the calls' room a step, as `take_marks` takes the S3 door's names; where a
    /// name found no room, every native job is woken, its word let go. A name whose job ended or
    /// went to another host finds none.
    fn take_calls(&mut self) {
        let Some(calls) = self.calls.as_mut() else {
            return;
        };
        let missed = calls
            .caller
            .missed
            .swap(false, std::sync::atomic::Ordering::AcqRel);
        let room = calls.room;
        if missed {
            for at in 0..self.jobs.len() {
                if let Some(Job {
                    door: Door::Native(_, native),
                    ..
                }) = self.jobs.get(at)
                {
                    native
                        .handed
                        .called
                        .store(false, std::sync::atomic::Ordering::Release);
                    self.wake(at);
                }
            }
        }
        for _ in 0..room {
            let Some(name) = self
                .calls
                .as_mut()
                .and_then(|calls| calls.taken.try_recv().ok().flatten())
            else {
                break;
            };
            let Some(at) = self.called.get(&name) else {
                continue;
            };
            if let Some(Job {
                door: Door::Native(_, native),
                ..
            }) = self.jobs.get(at)
            {
                native
                    .handed
                    .called
                    .store(false, std::sync::atomic::Ordering::Release);
            }
            self.wake(at);
        }
    }

    /// Job `at`'s keys from its state at `now`, the shard's clock: its beat or bound where its work
    /// is held (`Job::due_ns`), and its put's next renewal (`Job::renewal_ns`). Set as it is
    /// pushed, as its turn ends and as a piece of a chunk it asked moves: between its turns nothing
    /// else moves its state (the host's step answers it, offers it room and bounds it just before
    /// its turn). A key set at a turn can only come before the job's due at a later time, never
    /// after: a beat or vouch counted from its turn rather than from a later `now`, or a bound
    /// that passed with its reply begun, after which it needs no turn. Such a key costs one turn,
    /// which keys it again.
    ///
    /// A job whose chunk waits for room is kept with the waiters (`rooms`), keyed by when it began
    /// to wait, which it keeps while it waits.
    fn rekey(&mut self, at: usize, now: u64) {
        let Some(job) = self.jobs.get(at) else {
            return;
        };
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap_or(u64::MAX);
        let due = job.beat_ns(now, beat);
        let renewal = job.renewal_ns();
        let waiting = job
            .waits_for_room()
            .then(|| self.rooms.key(at).unwrap_or(now));
        let handing =
            matches!(job.work, Work::Handing(_)).then(|| self.handovers.key(at).unwrap_or(now));
        self.beats.set(at, due);
        self.renewals.set(at, renewal);
        self.rooms.set(at, waiting);
        self.handovers.set(at, handing);
    }

    /// The slot of the job of the native request on `exchange`.
    pub(super) fn job_of(&self, exchange: ExchangeId) -> Option<usize> {
        self.exchanges.get(&exchange)
    }

    /// Bytes its jobs moved since it was made.
    pub(super) fn moved(&self) -> u64 {
        self.moved
    }

    /// Whether it runs no job.
    pub(super) fn idle(&self) -> bool {
        self.jobs.is_empty()
    }

    /// Takes a native request handed with its fields read: its job made here, its work made from
    /// its operation (node.md §2.9 step 6e), its door naming `exchange` where the node's task runs
    /// it and none on a driving shard.
    pub(super) fn take_handed(&mut self, handed: Box<Handover>, exchange: Option<ExchangeId>) {
        let Handover {
            half,
            principal,
            op,
            block,
            deadline_ns,
            admitted_ns,
            charge,
            window,
            driven,
        } = *handed;
        // Named to the host it came from, or to none, before: its first step here, which comes as
        // it is pushed, sees what moved its door, and what moves it after names it here.
        half.handed
            .called
            .store(false, std::sync::atomic::Ordering::Release);
        let label = Some(labels::host(&op));
        let now = hyper_rt::futures::now_ns();
        self.push(
            Job {
                door: Door::Native(exchange, half),
                principal,
                work: work(op, block),
                pending: Vec::new(),
                deadline_ns,
                moved_ns: now,
                vouched_ns: None,
                resting: false,
                admitted_ns,
                charge,
                window,
                driven,
                label,
            },
            now,
        );
    }

    /// The first native job whose fields were read for a driving shard (`Work::Handing`), taken
    /// out to be handed there: its request and its exchange. Found among the handovers alone
    /// (`handovers`), so a turn's handing reads the jobs it hands and no other, where a pass of
    /// every job looked for them at each turn the node drives shards.
    fn handing(&mut self) -> Option<(Box<Handover>, Option<ExchangeId>)> {
        let (_, at) = self.handovers.least()?;
        #[cfg(test)]
        tests::READS.with(|reads| reads.set(reads.get().saturating_add(1)));
        let job = self.remove(at)?;
        let (Door::Native(exchange, half), Work::Handing(handing)) = (job.door, job.work) else {
            return None;
        };
        let (op, block) = *handing;
        Some((
            Box::new(Handover {
                half,
                principal: job.principal,
                op,
                block,
                deadline_ns: job.deadline_ns,
                admitted_ns: job.admitted_ns,
                charge: job.charge,
                window: job.window,
                driven: job.driven,
            }),
            exchange,
        ))
    }

    /// When, on the shard's clock, the host next needs a turn of whoever serves it, by its own
    /// jobs alone (node.md §2.9 step 6e), so a driving shard's task waits on its host and nothing
    /// of the node's: at once where what a job asked is answered (`Serve::answers`), as an answer
    /// that came after the host's step in a turn waits on no event; else a put's renewal coming
    /// due, or the beat or bound of a job whose work is held. Each operation at work is ended at
    /// its client's patience, and one whose asks are held says so each beat, a probe timeout, the
    /// transport's notes' beat of a third of the serve period (research/77 D2).
    ///
    /// Asked after every turn of the node's busiest task, it reads no job: each answer waiting is
    /// known for a job's by one probe of its asks' slots, and the earliest renewal and beat are
    /// each kept first in a heap (`renewals`, `beats`), each job keyed as its turn ends
    /// (`Host::rekey`), a key at most early, which costs one turn.
    pub(super) fn due<N: Serve>(&self, node: &N) -> Option<u64> {
        let shard = hyper_rt::futures::now_ns();
        if node
            .answers()
            .any(|pending| self.asks.get(&pending).is_some())
        {
            return Some(shard);
        }
        let renewal = self
            .renewals
            .first()
            .map(|due| shard.saturating_add(due.saturating_sub(node.wall_ns())));
        [renewal, self.beats.first()].into_iter().flatten().min()
    }

    /// The job waiting on chunk ask `ask`, its work moved now: a piece of the chunk it reads came,
    /// or one of the chunk it sends went (research/77 D2). Found by its ask in one probe of its
    /// asks' slots: a piece moves many times a chunk, and a search of every job's asks at each was
    /// the node's task's cost for every job in flight.
    pub(super) fn chunk_moved(&mut self, ask: u64) {
        let Some(at) = self.asks.get(&Pending::Chunk(ask)) else {
            return;
        };
        let Some(job) = self.jobs.get_mut(at) else {
            return;
        };
        let now = hyper_rt::futures::now_ns();
        job.moved_ns = now;
        self.rekey(at, now);
    }
}

/// What the host keeps of one native put or get beside its charge, by design: its job, and the
/// work it boxes, a put's or a get's whichever is larger, its driver inline in it; its reply's
/// door, the job's end and the node's task's, and what the two share; and a door kept for a next
/// request, the host keeping at most one an operation (`Doors`, at most `objects`) (node.md §2.9
/// step 6d; `derive::native`).
pub(crate) fn record_bytes() -> Option<usize> {
    let door = size_of::<Native>().checked_add(size_of::<Handed>())?;
    size_of::<Job>()
        .checked_add(size_of::<Putting>().max(size_of::<Getting>()))?
        .checked_add(door)?
        .checked_add(size_of::<(ExchangeId, Replies)>())?
        .checked_add(door)?
        .checked_add(size_of::<(Box<Native>, Replies)>())
}

/// The shard's clock (`hyper_rt::futures::now_ns`), as a turn of the host or of the node's native
/// replies reads it: once for the turn, and once after each step that did work ([`Timing`]). A
/// test counts the reads its thread made ([`tests::clock_reads`]).
fn clock() -> u64 {
    #[cfg(test)]
    tests::CLOCK_READS.with(|reads| reads.set(reads.get().saturating_add(1)));
    hyper_rt::futures::now_ns()
}

/// A turn's steps timed for its label (`crate::labels`): the clock is read once as the turn begins
/// and again only after a step that did work, a byte moved, an ask made or its job ended, each such
/// step charged the time since the last read. A step that found nothing to do reads no clock: on
/// the node's task a turn steps every job in flight, thousands, and reading the clock around each
/// was a tenth of the shard (macOS `sample` of the 12k/s PUT bench, 2026-11-19: `mach_absolute_time`
/// 11.7%, almost all of it there); its nanoseconds go to the next step that did work.
struct Timing {
    /// When, on the shard's clock, the last timed step ended, or the turn began.
    last_ns: u64,
    /// The step that held the turn longest, and its operation's label.
    slowest: Option<(u64, &'static Label)>,
}

impl Timing {
    /// A turn's timing, begun at `now`.
    fn from(now: u64) -> Self {
        Self {
            last_ns: now,
            slowest: None,
        }
    }

    /// A step of `label`'s operation that did work just ended: the time since the last is its.
    fn worked(&mut self, label: Option<&'static Label>) {
        let now = clock();
        let held = now.saturating_sub(self.last_ns);
        self.last_ns = now;
        if let Some(label) = label
            && self.slowest.is_none_or(|(most, _)| held >= most)
        {
            self.slowest = Some((held, label));
        }
    }
}

/// What a held job's bound makes of it at a turn (research/77 D2).
#[derive(Debug, PartialEq, Eq)]
enum Bounded {
    /// Within its client's patience: its client is told its work is held.
    Held,
    /// Past it, no reply begun: ended `503 SlowDown`, letting go of what it asked.
    SlowDown,
    /// Past it, its reply begun: told nothing more, its chunk reads ended by their transport.
    Unvouched,
}

/// Where a job's operation came in: its body is read and its reply written there.
enum Door {
    /// A native exchange (`mantle_protocol::object`), the operation's fields at its body's start,
    /// read from it at the job's turns; then a put's body given by the node's task ([`Feeding`]) and
    /// the reply written by the node's task ([`Replies`]), each from a bounded channel, as the S3
    /// listener gives a local door's body and writes its reply (node.md §2.9 step 6c). The exchange
    /// is the node's task's alone, none where the request was handed with its fields read (§2.9
    /// step 6e): only hyper-transport makes an `ExchangeId`, and the job needs none, its body and
    /// reply going through its door.
    Native(Option<ExchangeId>, Box<Native>),
    /// The S3 listener's, in-process (`crate::door`).
    Local(Box<Local>),
}

impl Door {
    /// The native exchange the operation came in on, which its reply is written to; none for the
    /// S3 listener's, or for a request handed with its fields read.
    fn exchange(&self) -> Option<ExchangeId> {
        match self {
            Self::Native(exchange, _) => *exchange,
            Self::Local(_) => None,
        }
    }

    /// The S3 request's name its connection names it by (`door::Mark`); none for a native door's,
    /// or a local one made with none.
    fn named(&self) -> Option<u64> {
        match self {
            Self::Local(local) => local.mark.as_ref().map(|mark| mark.id()),
            Self::Native(..) => None,
        }
    }

    /// The name a native job's calls name it by (`Call`), its door's half's, which a job handed
    /// to a driving shard carries; none for the S3 listener's.
    fn called(&self) -> Option<u64> {
        match self {
            Self::Native(_, native) => native.name,
            Self::Local(_) => None,
        }
    }
}

/// A native job's ends of its exchange's channels (node.md §2.9 step 6c): a put's body as the
/// node's task gives it, and its reply as the job gives it, which the node's task writes, with what
/// the two share beside it ([`Handed`]). The job touches nothing of the node's but these: the bytes
/// it moves it counts here, which the host's step takes as a value (`Host::turn_job`).
pub(crate) struct Native {
    /// A put's body, once its fields named one, on the channel made with its door and kept with
    /// it ([`Doors`]): its sender is the node's task's half's ([`Replies`]), lent to the put's
    /// feeding while the node's task reads the body ([`Feeding`]).
    body: Pieces,
    reply: Sender<Told>,
    /// Answers the reply's channel had no room for, sent before any other, in order. A piece is
    /// given only once the last is written whole, so these are at most the reply's end behind its
    /// last piece.
    pending: VecDeque<Told>,
    handed: std::sync::Arc<Handed>,
    /// Bytes of its body taken and of its reply given since the host's step last took them.
    moved: u64,
    /// The node's task's waker while the job runs on a driving shard (node.md §2.9 step 6f),
    /// woken as the job gives its door anything the node's task acts on: an answer to write, its
    /// work held, its body let go of, or a piece of its body taken, the next to be read. None
    /// where the node's task runs the job, which steps it in its own turn. A hyper-rt waker is a
    /// word its clone copies, so holding it costs nothing.
    node: Option<std::task::Waker>,
    /// The exchange its request came on, and the node's marks, on which the job names it as it
    /// gives its door anything the node's task acts on ([`Native::mark`]), so the node's turn
    /// visits its reply and its body's feeding alone; none for a job no node's task writes for.
    /// The marker is kept with the door, the exchange taken with each request.
    exchange: Option<ExchangeId>,
    marker: Option<Marker>,
    /// Its name on the calls of the host that runs it ([`Call`]), given as the node's task takes
    /// its request, unique for the node's life, and carried to a driving shard with the job; none
    /// for a job no node's task names.
    name: Option<u64>,
}

/// What a native job tells the node's task to write to its exchange, in order, as an S3 job tells
/// its connection (`door::Answer`): its reply's head and the fields that open its body, then a
/// get's pieces and its end.
enum Told {
    /// A reply with no bytes past its fields, written once the request's body is read whole and
    /// dropped, so the exchange ends whole (`Arriving::Redirect`'s reason), and the exchange ended
    /// once its fields are written.
    Reply(Box<Opening>),
    /// A get's reply, held until its driver had the range's first bytes: its head now, its pieces
    /// to follow.
    Head(Box<Opening>),
    /// A get's bytes, in order, as its driver gave them.
    Bytes(Bytes),
    /// Every byte given and written: the exchange ends.
    End,
    /// The exchange is refused for the cause it carries: a get's driver failed past its head, so
    /// the exchange ends unfinished, which its asker hears as the exchange refused, not as bytes it
    /// could take for the object's; or a reply no message holds, a field past its bound, which its
    /// asker hears at once rather than a period's silence.
    Failed(Refusal),
}

/// A reply's head and the fields that open its body (`Reply::encode`), the job's work; the bytes of
/// the body past the fields, a get's range and none for any other reply, which the head's body
/// length counts (`Endpoint::reply`); and the cause of a `SlowDown`, which the node counts as the
/// reply goes, as the listener counts the S3 door's.
struct Opening {
    head: Vec<u8>,
    fields: Bytes,
    body: u64,
    slowed: Option<door::Slowed>,
    /// When, on the shard's clock, its job's client stops waiting, as the job's bound stood when it
    /// gave the reply (`Job::bound_ns`): a reply whose piece its request's body drains through
    /// waits for room in the endpoint's budget is held until then, and past it its exchange is
    /// refused `Budget` (`Replies::wait_for_room`). Its head never waits: it fills the room the
    /// endpoint took with its request's head (`node::reply_head`).
    bound_ns: u64,
    /// Whether its job was admitted, charged a piece of its body: what is left of the body is read
    /// into the node's task's piece rather than the endpoint's (`Node::drop_body`).
    admitted: bool,
}

/// What a native job and the node's task share of its reply beside its channel, each an atomic that
/// needs no lock, as the S3 door's connections tell the host what they wrote (`door::Learned`): the
/// fields and pieces the job gave that the node's task has yet to write whole, past which the job
/// gives none, so a get holds one piece taken from its driver, as it held one while its job wrote
/// its exchange (`derive::Charges`: "a get's block taken and written out"); and whether its work is
/// held, which the node's task tells the exchange's asker (`Endpoint::working`, research/77 D1).
/// And whether the node's task let go of the reply, its exchange ended or refused, so a job waiting
/// for what it gave to be written ends; and whether the job let go of its door, so the node's task
/// writes nothing past what it gave: a door kept for the next request ([`Doors`]) closes neither
/// end of its channels, which once said both. And of a put's body, what its channel's ends'
/// drops said: whether the job let go of it (`dropped`), so the node's task reads and drops the
/// rest, and whether the node's task let go of it (`cut`), so the job, finding nothing more given,
/// knows its body did not come whole, the word read before the channel so a piece given before it
/// is taken first.
/// And whether the job named its exchange on the node's marks since the node's task took the
/// last ([`Native::mark`]), so it names it once however much it gives meanwhile: the marks hold
/// each exchange once.
#[derive(Default)]
struct Handed {
    unwritten: std::sync::atomic::AtomicUsize,
    held: std::sync::atomic::AtomicBool,
    gone: std::sync::atomic::AtomicBool,
    ended: std::sync::atomic::AtomicBool,
    dropped: std::sync::atomic::AtomicBool,
    cut: std::sync::atomic::AtomicBool,
    marked: std::sync::atomic::AtomicBool,
    /// The other way ([`Call`]): whether the node's task names the job to the host that runs it as
    /// it moves its door, so the job's turns pass it by while it waits on its door alone
    /// (`Job::rests`); and whether the node's task named it since that host took the last name, so
    /// it names it once however much it moves the door meanwhile.
    calls: std::sync::atomic::AtomicBool,
    called: std::sync::atomic::AtomicBool,
    /// The job's name on those calls (`Native::name`), which the node's task names it by.
    name: std::sync::atomic::AtomicU64,
    /// The oracle's counts, each moved after the job's mark: the answers on the reply's channel
    /// not yet taken, and the pieces of the body the job took since its feeding last looked
    /// (`Writes::audit`, `Feedings::audit`).
    #[cfg(test)]
    told: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    took: std::sync::atomic::AtomicUsize,
}

impl Native {
    /// Sends what waits, in order: whether the door takes more now; `Gone` once the node's task let
    /// go of the reply, whose channel's receiving end may be kept for the next request ([`Doors`]).
    fn flush(&mut self) -> Sent {
        if self.gone() {
            return Sent::Gone;
        }
        let waiting = self.pending.len();
        let sent = flush(&self.reply, &mut self.pending);
        if self.pending.len() < waiting {
            self.mark();
            #[cfg(test)]
            self.handed.told.fetch_add(
                waiting.saturating_sub(self.pending.len()),
                std::sync::atomic::Ordering::AcqRel,
            );
            self.wake_node();
        }
        sent
    }

    /// Sends `told` after what waits, or holds it behind them.
    fn offer(&mut self, told: Told) -> Sent {
        self.pending.push_back(told);
        self.flush()
    }

    /// Gives fields or a piece the node's task writes: counted unwritten until it is written whole.
    fn give(&mut self, told: Told) -> Sent {
        self.handed
            .unwritten
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        self.offer(told)
    }

    /// Whether every field and piece it gave is written whole, and nothing waits: the next may be
    /// taken from its driver.
    fn written(&self) -> bool {
        self.pending.is_empty()
            && self
                .handed
                .unwritten
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
    }

    /// Whether the node's task let go of its reply: nothing it gives will be written.
    fn gone(&self) -> bool {
        self.handed.gone.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Whether the node's task names its job as it moves its door ([`Call`]) and holds its reply
    /// still: what the door gives it next wakes it.
    fn called(&self) -> bool {
        self.handed.calls.load(std::sync::atomic::Ordering::Acquire) && !self.gone()
    }

    /// Says its work is held: the node's task vouches for it at its next write, woken where it
    /// runs on a driving shard only where the node's task took the last word, as it vouches once
    /// for each it takes.
    fn held(&self) {
        if !self
            .handed
            .held
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            self.mark();
            self.wake_node();
        }
    }

    /// Lets go of its body: the node's task reads and drops the rest.
    fn drop_body(&self) {
        self.handed
            .dropped
            .store(true, std::sync::atomic::Ordering::Release);
        self.mark();
        self.wake_node();
    }

    /// Names its exchange on the node's marks, once until the node's task takes it: its reply or
    /// its body's feeding may move. The marks have room for each exchange the transport holds
    /// twice over ([`Marks::new`]); a name they have none for is said beside them, and the node's
    /// task visits every reply and feeding.
    fn mark(&self) {
        let (Some(exchange), Some(marker)) = (self.exchange, self.marker.as_ref()) else {
            return;
        };
        if self
            .handed
            .marked
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        if marker.marks.try_send(exchange).is_err() {
            marker
                .missed
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// Turns the node's task where the job runs on a driving shard, after what it gave its door.
    fn wake_node(&self) {
        if let Some(node) = &self.node {
            node.wake_by_ref();
        }
    }
}

/// What a get's door takes its bytes from: its driver (`Get`), or a test's.
trait Source {
    /// The next of its bytes, in order, once ready.
    fn take(&mut self) -> Option<Bytes>;
    /// Whether it ended whole, once it ended.
    fn ended(&self) -> Option<bool>;
}

impl Source for Get {
    fn take(&mut self) -> Option<Bytes> {
        Get::take(self)
    }

    fn ended(&self) -> Option<bool> {
        self.outcome().map(Result::is_ok)
    }
}

/// What became of a native get's turn at its door.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gave {
    /// It waits: for the node's task to write what it gave, or for its driver.
    Waits,
    /// Its end is given, or its door is gone.
    Over,
    /// Its end waits for room on the channel.
    Ending,
}

/// A native get's bytes given to its door from `source`, `written` of its `total` given so far: a
/// piece only once the node's task wrote the last whole (`Native::written`), so the get holds one
/// piece past its driver's window, as when its job wrote its exchange, and the node's task asks for
/// the next within its turn while the exchange takes more (`Node::write_replies`); then its end, or
/// its failure past its head, once every piece ready was given.
fn give_native(
    native: &mut Native,
    source: &mut impl Source,
    written: &mut u64,
    total: u64,
) -> Gave {
    match native.flush() {
        Sent::Taken if native.gone() => return Gave::Over,
        Sent::Taken => {}
        Sent::Held => return Gave::Waits,
        Sent::Gone => return Gave::Over,
    }
    if !native.written() {
        return Gave::Waits;
    }
    if let Some(bytes) = source.take() {
        let took = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        *written = written.saturating_add(took);
        native.moved = native.moved.saturating_add(took);
        return match native.give(Told::Bytes(bytes)) {
            Sent::Gone => Gave::Over,
            Sent::Taken | Sent::Held => Gave::Waits,
        };
    }
    let last = match source.ended() {
        Some(true) if *written >= total => Told::End,
        // The driver failed past the reply's head: the exchange ends unfinished.
        Some(false) => Told::Failed(Refusal::Closed),
        _ => return Gave::Waits,
    };
    match native.offer(last) {
        Sent::Held => Gave::Ending,
        Sent::Taken | Sent::Gone => Gave::Over,
    }
}

/// A native exchange's door, made as the host takes its request: the job's ends, and the node's
/// task's ([`Replies`]); none where its reply's channel or its body's cannot be made.
fn native_door() -> Option<(Native, Replies)> {
    let (reply, told) = hyper_rt::sync::channel(door::Replying::FIRST_ANSWERS).ok()?;
    let (body, pieces) = hyper_rt::sync::channel(door::Depth::FIRST).ok()?;
    let handed = std::sync::Arc::new(Handed::default());
    Some((
        Native {
            body: Pieces {
                pieces,
                ended: None,
                dry: false,
            },
            reply,
            pending: VecDeque::new(),
            handed: std::sync::Arc::clone(&handed),
            moved: 0,
            node: None,
            exchange: None,
            marker: None,
            name: None,
        },
        Replies {
            told,
            body: Some(body),
            handed,
            drained: None,
            out: None,
            drain: Drain::default(),
            room: None,
            call: None,
        },
    ))
}

/// The native doors the host keeps for its next requests (node.md §2.9 step 6d), so a request takes
/// its reply's channel, and what its job and the node's task share beside it ([`Handed`]), from
/// those kept rather than from the allocator. A door's two halves are each given back as it is let
/// go: the job's ([`Native`]) as its job ends, the node's task's ([`Replies`]) once its reply is
/// written whole or refused, or its exchange ended; and a door both of whose halves are back is
/// emptied of what its job gave and the node's task never took, its shared words cleared, and kept.
/// A half given back first waits for the other, which is then still out, a job's or a reply the
/// node's task writes, each of a request the node holds. Bound: the doors kept, at most `most`,
/// the operations the host runs at once (`GatewaySettings::objects`), one an operation, as the
/// memory floor counts them (`record_bytes`), and never past the exchanges the transport holds at
/// once (the node's `parked_most`), as each door out is of an exchange the node holds; one given
/// back past them is let go.
#[derive(Default)]
struct Doors {
    kept: Vec<(Box<Native>, Replies)>,
    /// Halves given back whose door's other half is still out.
    waiting: Vec<Half>,
    most: usize,
}

/// A door's half given back.
enum Half {
    Native(Box<Native>),
    Replies(Replies),
}

impl Half {
    /// What its door's two halves share, which tells the door.
    fn handed(&self) -> &std::sync::Arc<Handed> {
        match self {
            Self::Native(native) => &native.handed,
            Self::Replies(replies) => &replies.handed,
        }
    }
}

impl Doors {
    /// Doors for a host that keeps at most `most` of them.
    fn new(most: usize) -> Self {
        Self {
            kept: Vec::new(),
            waiting: Vec::new(),
            most,
        }
    }

    /// A door for a request: one kept, or one made; none where its channel cannot be made.
    fn take(&mut self) -> Option<(Box<Native>, Replies)> {
        if let Some(door) = self.kept.pop() {
            return Some(door);
        }
        let (native, replies) = native_door()?;
        Some((Box::new(native), replies))
    }

    /// A job's half of its door, the job ended: the node's task writes nothing past what it gave.
    fn give_native(&mut self, native: Box<Native>) {
        native
            .handed
            .ended
            .store(true, std::sync::atomic::Ordering::Release);
        native.mark();
        self.give(Half::Native(native));
    }

    /// The node's task's half of a door, its reply written whole or refused, or its exchange
    /// ended: its job gives nothing more.
    fn give_replies(&mut self, replies: Replies) {
        replies
            .handed
            .gone
            .store(true, std::sync::atomic::Ordering::Release);
        replies.wake_job();
        self.give(Half::Replies(replies));
    }

    /// A half given back: its door kept once the other half is back too, else held for it.
    fn give(&mut self, half: Half) {
        let other = self
            .waiting
            .iter()
            .position(|waiting| std::sync::Arc::ptr_eq(waiting.handed(), half.handed()));
        let Some(at) = other else {
            self.waiting.push(half);
            return;
        };
        match (half, self.waiting.swap_remove(at)) {
            (Half::Native(native), Half::Replies(replies))
            | (Half::Replies(replies), Half::Native(native)) => self.keep(native, replies),
            // A door has one half of each: two of one are none of a door's, and go.
            _ => {}
        }
    }

    /// A door both of whose halves are back, kept for the next request where fewer than `most`
    /// are: what its job gave that the node's task never took dropped, and what the node's task
    /// gave of a body that its job never took, at most each channel's room, and its halves as a
    /// new door's. One whose body's sender is not back, which a feeding let go of gives back
    /// first, is let go.
    fn keep(&mut self, mut native: Box<Native>, mut replies: Replies) {
        if self.kept.len() >= self.most || replies.body.is_none() {
            return;
        }
        if !emptied(&mut replies.told, door::Replying::FIRST_ANSWERS)
            || !emptied(&mut native.body.pieces, door::Depth::FIRST)
        {
            return;
        }
        native.body.ended = None;
        native.pending.clear();
        native.moved = 0;
        native.node = None;
        native.exchange = None;
        native.name = None;
        replies.drained = None;
        replies.out = None;
        replies.drain = Drain::default();
        replies.room = None;
        replies.call = None;
        let handed = &native.handed;
        handed
            .unwritten
            .store(0, std::sync::atomic::Ordering::Release);
        for word in [
            &handed.held,
            &handed.gone,
            &handed.ended,
            &handed.dropped,
            &handed.cut,
            &handed.marked,
            &handed.calls,
            &handed.called,
        ] {
            word.store(false, std::sync::atomic::Ordering::Release);
        }
        #[cfg(test)]
        for count in [&handed.told, &handed.took] {
            count.store(0, std::sync::atomic::Ordering::Release);
        }
        self.kept.push((native, replies));
    }
}

/// Whether a kept door's channel is emptied of what was given and never taken, at most `room`:
/// none where the channel is closed, its other end gone.
fn emptied<T>(channel: &mut ChannelReceiver<T>, room: usize) -> bool {
    for _ in 0..=room {
        match channel.try_recv() {
            Ok(Some(_)) => {}
            Ok(None) => return true,
            Err(_) => return false,
        }
    }
    false
}

/// A native job's reply as the node's task writes it to the exchange (node.md §2.9 step 6c), the
/// exchange's half of what the S3 listener does for a local door's reply: each answer taken from
/// the job's channel in order and written, a reply's head with its body's length
/// (`Endpoint::reply`), then its fields and a get's pieces as the job gave them
/// (`Endpoint::write_body_bytes`, no copy), then its end; a reply with no bytes past its fields
/// written once the request's body is read whole, after its feeding (`Node::feed_body`); and the
/// job's word that its work is held passed to the exchange's asker. The channel holds one
/// answer, the least that moves a reply (`door::Replying::FIRST_ANSWERS`), and the job gives a
/// piece only once the last is written whole ([`Handed`]), so a full exchange holds the job, and
/// the client, whose stream's flow control it is, holds both.
pub(crate) struct Replies {
    told: ChannelReceiver<Told>,
    /// A put's body's sender, kept with its door, none while the put's feeding holds it
    /// ([`Feeding`]).
    body: Option<Sender<Piece>>,
    handed: std::sync::Arc<Handed>,
    /// A reply taken that waits for the request's body to be read whole, or for room in the
    /// endpoint's budget for the piece that body drains through.
    drained: Option<Box<Opening>>,
    /// Bytes taken and not yet written whole, and what follows once they are.
    out: Option<(Bytes, After)>,
    /// What a get's client took of the piece being written once its room was full ([`Drain`]).
    drain: Drain,
    /// While the piece its request's body drains through waits for room in the endpoint's budget,
    /// its client's patience (`Opening::bound_ns`) and when, on the shard's clock, its exchange's
    /// asker was last told its work is held: offered again as the budget gives bytes back, at the
    /// turn it does (`Writes::room_freed`; `Endpoint::release` retries only the transport's
    /// lossless frames, and tells its owner nothing), and its exchange vouched for half a beat
    /// apart meanwhile (`vouch_every`), its node's turn due at its next vouch (research/77 D1, D2;
    /// `Node::host_due`); past it, its exchange refused `Budget`.
    room: Option<(u64, u64)>,
    /// The caller of the host that runs its job, the node's own or a driving shard's
    /// ([`Caller`]): told as the node's task takes what the job gave or writes it whole, or lets
    /// go of the reply, so the job gives its next or ends, it names the job to that host and wakes
    /// a driving shard's task (node.md §2.9 step 6f); none for a job no host is told of, which its
    /// host steps at every turn.
    call: Option<std::sync::Arc<Caller>>,
}

/// What a native get's client took of a piece the node's task held whole, timed from when the
/// client's room for it was full, so its client's pace with no window holding it back
/// (`crate::window`): a piece its room took at once says only that the room was there. The room is
/// the stream's flow-control window, smaller than a block, so each block's writing waits on its
/// client. On the S3 door the reply's channel holds whole blocks, so the connection times the same
/// of its socket as it writes each (`listener::io::Drained`), and the node takes it from the door
/// (`door::Doorway::drained`).
#[derive(Debug, Clone, Copy, Default)]
struct Drain {
    /// When, on the shard's clock, the client's room was full; none while it was not.
    held_ns: Option<u64>,
    /// The bytes it took since.
    bytes: u64,
    /// The last piece's pace once it was written whole, its bytes and their nanoseconds, for the
    /// node's task to take (`Replies::take_paced`).
    paced: Option<(u64, u64)>,
}

impl Drain {
    /// `took` bytes of the piece written at `now`, the last of them where `whole`: counted while
    /// its client's room was full, which it was from now where it took less than the rest; and
    /// once whole, what its client took since its room was full kept as its pace.
    fn wrote(&mut self, took: u64, whole: bool, now: u64) {
        if self.held_ns.is_some() {
            self.bytes = self.bytes.saturating_add(took);
        }
        if !whole {
            self.held_ns.get_or_insert(now);
            return;
        }
        if let Some(held) = self.held_ns.take() {
            self.paced = Some((self.bytes, now.saturating_sub(held)));
        }
        self.bytes = 0;
    }
}

/// What follows bytes of a reply written whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum After {
    /// A get's fields, before its pieces.
    Fields,
    /// A get's piece, which its job counted moved as it gave it.
    Piece,
    /// A reply's fields, its last bytes: the exchange ends.
    End,
}

/// What became of a native reply the node's task wrote at a turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wrote {
    /// Nothing more now: the exchange takes no more, or the job gave nothing more.
    Still,
    /// Everything given is written whole and the exchange takes more: its job may give the next.
    More,
    /// A reply waits for the request's body, read whole and dropped.
    Draining,
    /// A reply waits for room in the endpoint's budget, its exchange vouched for.
    Room,
    /// The exchange ended, or was refused, or its job let go with nothing more to write.
    Over,
}

/// The exchange a native reply is written to: the node's endpoint, or a test's.
trait Exchange {
    /// Answers with `head` and a body of `body` bytes: what became of the head (`node::Replied`).
    fn reply(&mut self, head: &[u8], body: u64) -> super::Replied;
    /// Writes `bytes` as they are, kept by reference: how many it took, none where it is gone.
    fn write(&mut self, bytes: &Bytes) -> Option<usize>;
    fn end(&mut self);
    /// Ends it unfinished, refused for `refusal`.
    fn refuse(&mut self, refusal: Refusal);
    /// Tells its asker the work for it is held.
    fn working(&mut self);
}

/// A native exchange on the node's endpoint.
struct Toward<'a> {
    endpoint: &'a mut hyper_transport::Endpoint<
        mantle_protocol::classes::NodeClasses,
        hyper_transport::Fixed,
        crate::directory::Peers,
    >,
    exchange: ExchangeId,
}

impl Exchange for Toward<'_> {
    fn reply(&mut self, head: &[u8], body: u64) -> super::Replied {
        super::reply_head(self.endpoint, self.exchange, head, Some(body))
    }

    fn write(&mut self, bytes: &Bytes) -> Option<usize> {
        self.endpoint.write_body_bytes(self.exchange, bytes).ok()
    }

    fn end(&mut self) {
        self.endpoint.end(self.exchange);
    }

    fn refuse(&mut self, refusal: Refusal) {
        self.endpoint.refuse_exchange(self.exchange, refusal);
    }

    fn working(&mut self) {
        self.endpoint.working(self.exchange);
    }
}

impl Drop for Replies {
    /// Tells the job its reply is let go of, so one waiting for what it gave to be written ends.
    fn drop(&mut self) {
        self.handed
            .gone
            .store(true, std::sync::atomic::Ordering::Release);
        self.wake_job();
    }
}

impl Replies {
    /// Whether a reply waits for the request's body.
    fn draining(&self) -> bool {
        self.drained.is_some()
    }

    /// Whether the reply waiting for the request's body is of a job admitted, charged a piece of
    /// its body (`Opening::admitted`).
    fn admitted(&self) -> bool {
        self.drained
            .as_ref()
            .is_some_and(|opening| opening.admitted)
    }

    /// When, on the shard's clock, the node's next turn is due for a reply that waits for room in
    /// the endpoint's budget: at its next vouch, half a beat after its last, at most its client's
    /// patience (`room`). Its room is offered again then, and as the budget gives bytes back
    /// (`Writes::wake_rooms`).
    pub(super) fn room_due_ns(&self, beat: u64) -> Option<u64> {
        self.room
            .map(|(bound, vouched)| bound.min(vouched.saturating_add(vouch_every(beat))))
    }

    /// Names its job to the host that runs it and wakes the driving shard it runs on, where one
    /// does, while the job has not ended: its door moved.
    fn wake_job(&self) {
        if self.handed.ended.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        if let Some(call) = &self.call {
            call.name(&self.handed);
        }
    }

    /// Names its job to `call`'s host from here, as its feeding does where it holds the same: the
    /// job rests on its door only where one does (`Job::rests`). A name given another host before
    /// is let go as the host that takes the job pushes it (`Host::take_handed`).
    fn call_by(&mut self, call: Option<std::sync::Arc<Caller>>) {
        self.handed
            .calls
            .store(call.is_some(), std::sync::atomic::Ordering::Release);
        self.call = call;
    }

    /// Writes what the job gave, as [`Replies::write_told`] does, and wakes the driving shard its
    /// job runs on where an answer was taken or written whole: the job gives the next only then.
    fn write(
        &mut self,
        to: &mut impl Exchange,
        body_read: bool,
        now: u64,
        moved: &mut u64,
        slowed: &mut [u64],
    ) -> Wrote {
        let mut took = false;
        let wrote = self.write_told(to, (body_read, now), moved, slowed, &mut took);
        if took {
            self.wake_job();
        }
        wrote
    }

    /// Writes what the job gave, in order, as far as `to` takes it, the request's body read whole
    /// where `body_read`: the fields' bytes counted `moved`, and a `SlowDown` counted in `slowed`
    /// for its cause as it goes. Each pass writes an answer whole or ends, and the job gives the
    /// next only once the last is written whole, so a pass takes at most the channel's one answer
    /// and the end behind it. A head never waits for room: it fills the room the endpoint took
    /// with its request's head (`node::reply_head`). A get's piece is timed at `now` where its
    /// client's room was full ([`Drain`]). Whether an answer was taken from the job's channel or
    /// written whole it says in `took`.
    fn write_told(
        &mut self,
        to: &mut impl Exchange,
        (body_read, now): (bool, u64),
        moved: &mut u64,
        slowed: &mut [u64],
        took: &mut bool,
    ) -> Wrote {
        if self
            .handed
            .held
            .swap(false, std::sync::atomic::Ordering::Acquire)
        {
            to.working();
        }
        let mut whole = false;
        // The answer held, the channel's one, and the end behind it.
        for _ in 0..door::Replying::FIRST_ANSWERS.saturating_add(2) {
            if let Some(opening) = self.drained.take() {
                if !body_read {
                    self.drained = Some(opening);
                    return Wrote::Draining;
                }
                let length = u64::try_from(opening.fields.len()).unwrap_or(u64::MAX);
                if to.reply(&opening.head, length) == super::Replied::Ended {
                    return Wrote::Over;
                }
                self.room = None;
                if let Some(count) = opening
                    .slowed
                    .and_then(|cause| slowed.get_mut(cause.index()))
                {
                    *count = count.saturating_add(1);
                }
                self.out = Some((opening.fields, After::End));
            }
            if let Some((bytes, then)) = self.out.take() {
                if !bytes.is_empty() {
                    let Some(took) = to.write(&bytes) else {
                        to.refuse(Refusal::Closed);
                        return Wrote::Over;
                    };
                    let took_bytes = u64::try_from(took).unwrap_or(u64::MAX);
                    if then != After::Piece {
                        *moved = moved.saturating_add(took_bytes);
                    }
                    if took < bytes.len() {
                        if then == After::Piece {
                            self.drain.wrote(took_bytes, false, now);
                        }
                        self.out = Some((bytes.slice(took..), then));
                        return Wrote::Still;
                    }
                    if then == After::Piece {
                        self.drain.wrote(took_bytes, true, now);
                    }
                }
                if then == After::End {
                    to.end();
                    return Wrote::Over;
                }
                self.handed
                    .unwritten
                    .fetch_sub(1, std::sync::atomic::Ordering::Release);
                whole = true;
                *took = true;
            }
            // Read before the channel, so what the job gave before it let go is taken first.
            let ended = self.handed.ended.load(std::sync::atomic::Ordering::Acquire);
            let told = self.told.try_recv();
            *took |= matches!(told, Ok(Some(_)));
            #[cfg(test)]
            if matches!(told, Ok(Some(_))) {
                self.handed
                    .told
                    .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            }
            match told {
                Ok(Some(Told::Reply(opening))) => self.drained = Some(opening),
                Ok(Some(Told::Head(opening))) => {
                    if let Some(wrote) = self.open(to, *opening) {
                        return wrote;
                    }
                }
                Ok(Some(Told::Bytes(bytes))) => self.out = Some((bytes, After::Piece)),
                Ok(Some(Told::End)) => {
                    to.end();
                    return Wrote::Over;
                }
                Ok(Some(Told::Failed(refusal))) => {
                    to.refuse(refusal);
                    return Wrote::Over;
                }
                // The job let go with nothing more to write: the exchange is left as it left it,
                // its door given back ([`Doors`]) or dropped.
                Ok(None) if ended => return Wrote::Over,
                Ok(None) if whole => return Wrote::More,
                Ok(None) => return Wrote::Still,
                Err(_) => return Wrote::Over,
            }
        }
        Wrote::Still
    }

    /// A get's head given, its fields to follow and its range's bytes past them, which the head's
    /// length counts: none where it went, else its exchange is over.
    fn open(&mut self, to: &mut impl Exchange, opening: Opening) -> Option<Wrote> {
        let length = u64::try_from(opening.fields.len())
            .unwrap_or(u64::MAX)
            .saturating_add(opening.body);
        match to.reply(&opening.head, length) {
            super::Replied::Sent => {
                self.out = Some((opening.fields, After::Fields));
                None
            }
            super::Replied::Ended => Some(Wrote::Over),
        }
    }

    /// The pace of the last of a get's pieces its client took once its room was full, its bytes
    /// and their nanoseconds, taken once ([`Drain`]).
    fn take_paced(&mut self) -> Option<(u64, u64)> {
        self.drain.paced.take()
    }

    /// The reply held, the piece its request's body drains through refused for room in the
    /// endpoint's budget: within its client's patience it waits, offered again as the budget gives
    /// bytes back (`Writes::wake_rooms`), and its exchange vouched for (`Endpoint::working`) at
    /// its first turn waiting and then half a beat after its last (`vouch_every`), as a held
    /// job's (`Job::vouch`): nothing of it moves while it waits, and a vouch at every turn said
    /// nothing more. Past it, its exchange is refused `Budget`, which needs no room and its asker
    /// hears at once, as it hears a request whose head the budget refused. A reply never ends with
    /// its exchange open and its asker waiting on nothing, which its client gave up a period later
    /// (`Lost(Stalled)`, tests/put_body.rs).
    fn wait_for_room(&mut self, to: &mut impl Exchange, now: u64, beat: u64) -> Wrote {
        let bound = self
            .drained
            .as_ref()
            .map_or(now, |opening| opening.bound_ns);
        if now < bound {
            let vouched = match self.room {
                Some((_, vouched)) if now < vouched.saturating_add(vouch_every(beat)) => vouched,
                _ => {
                    to.working();
                    now
                }
            };
            self.room = Some((bound, vouched));
            return Wrote::Room;
        }
        self.room = None;
        to.refuse(Refusal::Budget);
        Wrote::Over
    }
}

/// The node's task's halves of the native replies in flight (`Node::replies`), each found by its
/// exchange in one probe (`keyed::Keyed`), and each one's due while it waits for room in the
/// endpoint's budget (`Replies::room_due_ns`) a key in a heap by its slot, as the host keeps its
/// jobs' (`Earliest`): the node's wait (`Node::host_due`) reads the earliest in one step, where it
/// walked every reply. A reply's due is its own state's alone, the beat a constant, and a reply
/// changes only through [`Writes::change`] and [`Writes::change_at`], each keying it again after,
/// so every key is its reply's due: none is ever stale. Bound: one key a reply. Its exchanges'
/// type a parameter only so a test counts it with a stand-in for `ExchangeId`, which only the
/// transport makes.
pub(super) struct Writes<K = ExchangeId>
where
    (K, Replies): Key<Of = K>,
{
    replies: super::keyed::Readied<(K, Replies)>,
    due: Earliest,
    /// The bytes the endpoint's budget held when a reply's visit last found no room in it for the
    /// piece its request's body drains through (`Writes::short_of`); none since its waiting
    /// replies were last marked for room given back. A piece fits again only once the budget
    /// holds fewer, bytes given back (`Endpoint::release`, an exchange's end): the event a waiting
    /// reply waits on (`Writes::room_freed`), not each of the node's turns.
    short: Option<u64>,
}

impl<K> Default for Writes<K>
where
    (K, Replies): Key<Of = K>,
{
    /// None, which allocates nothing (`Readied::default`, `Earliest::default`).
    fn default() -> Self {
        Self {
            replies: super::keyed::Readied::default(),
            due: Earliest::default(),
            short: None,
        }
    }
}

impl<K: Copy> Writes<K>
where
    (K, Replies): Key<Of = K>,
{
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.replies.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.replies.is_empty()
    }

    pub(super) fn contains(&self, exchange: K) -> bool {
        self.replies.contains(exchange)
    }

    /// The reply at slot `at`, as [`super::keyed::Keyed::at`].
    pub(super) fn at(&self, at: usize) -> Option<&(K, Replies)> {
        self.replies.at(at)
    }

    /// The earliest due of the replies waiting for room, on the shard's clock.
    pub(super) fn first(&self) -> Option<u64> {
        self.due.first()
    }

    /// A reply's visit found no room in the endpoint's budget, which held `used` bytes.
    pub(super) fn short_of(&mut self, used: u64) {
        self.short = Some(used);
    }

    /// Whether a reply waits for room in the endpoint's budget and the budget, holding `used`
    /// bytes, gave bytes back since a visit last found it short: the one event that lets a waiting
    /// reply's piece fit, as one refused at `short` bytes fits only once fewer are held.
    pub(super) fn room_freed(&self, used: u64) -> bool {
        self.due.first().is_some() && self.short.is_some_and(|short| used < short)
    }

    /// Marks to visit the replies waiting for room in the endpoint's budget that move now: every
    /// one where the budget, holding `used` bytes, gave bytes back since a visit found it short
    /// (`room_freed`), else each whose due, its next vouch or its client's patience, is `now` or
    /// past. Each waiting one found in the heap of their dues, never by a walk of every reply, and
    /// none marked while nothing it waits on changed. How many it marked.
    pub(super) fn wake_rooms(&mut self, used: u64, now: u64) -> usize {
        let freed = self.room_freed(used);
        if !freed && self.due.first().is_none_or(|first| first > now) {
            return 0;
        }
        if freed {
            self.short = None;
        }
        let Self { replies, due, .. } = self;
        let mut woken = 0usize;
        for (key, at) in due.entries() {
            if !freed && *key > now {
                continue;
            }
            if let Some(exchange) = replies.at(*at).map(|(exchange, _)| *exchange) {
                replies.mark(exchange);
                woken = woken.saturating_add(1);
            }
        }
        #[cfg(test)]
        self.audit();
        woken
    }

    /// Holds `reply`, last, keyed by its due. One already held on its exchange, which no caller
    /// pushes (each holds its own), is replaced in its slot and given back.
    pub(super) fn push(&mut self, reply: (K, Replies)) -> Option<(K, Replies)> {
        let (exchange, due) = (reply.key(), reply.1.room_due_ns(room_beat()));
        let displaced = self.replies.push(reply);
        let at = match displaced {
            None => {
                self.due.grow();
                self.replies.len().checked_sub(1)
            }
            Some(_) => self.replies.slot(exchange),
        };
        if let Some(at) = at {
            self.due.set(at, due);
        }
        #[cfg(test)]
        self.audit();
        displaced
    }

    /// The reply on `exchange` changed by `change`, and keyed again by its due.
    pub(super) fn change<R>(
        &mut self,
        exchange: K,
        change: impl FnOnce(&mut Replies) -> R,
    ) -> Option<R> {
        let at = self.replies.slot(exchange)?;
        self.change_at(at, change)
    }

    /// The reply at slot `at` changed by `change`, and keyed again by its due.
    pub(super) fn change_at<R>(
        &mut self,
        at: usize,
        change: impl FnOnce(&mut Replies) -> R,
    ) -> Option<R> {
        let (_, replies) = self.replies.at_mut(at)?;
        let changed = change(replies);
        let due = replies.room_due_ns(room_beat());
        self.due.set(at, due);
        #[cfg(test)]
        self.audit();
        Some(changed)
    }

    /// The reply at slot `at` taken out, the last moved into its slot, its key with it.
    pub(super) fn take(&mut self, at: usize) -> Option<(K, Replies)> {
        let reply = self.replies.take(at)?;
        self.due.swap_remove(at);
        #[cfg(test)]
        self.audit();
        Some(reply)
    }

    /// The reply on `exchange` taken out, as [`Writes::take`].
    pub(super) fn remove(&mut self, exchange: K) -> Option<(K, Replies)> {
        let at = self.replies.slot(exchange)?;
        self.take(at)
    }

    /// Marks the reply on `exchange` to visit at the next writing: something it waits on came.
    pub(super) fn mark(&mut self, exchange: K) {
        self.replies.mark(exchange);
        #[cfg(test)]
        self.audit();
    }

    /// Marks the reply on `exchange`, which its job named on the node's marks, then lets go of
    /// its door's word, so what its job gives after names it again ([`Native::mark`]).
    pub(super) fn marked_by_job(&mut self, exchange: K) {
        self.replies.mark(exchange);
        if let Some((_, replies)) = self
            .replies
            .slot(exchange)
            .and_then(|at| self.replies.at(at))
        {
            replies
                .handed
                .marked
                .store(false, std::sync::atomic::Ordering::Release);
        }
        #[cfg(test)]
        self.audit();
    }

    /// Marks every reply and lets go of each door's word: a mark found no room
    /// ([`Marks::missed`]).
    pub(super) fn mark_all(&mut self) {
        self.replies.mark_all();
        for (_, replies) in self.replies.iter() {
            replies
                .handed
                .marked
                .store(false, std::sync::atomic::Ordering::Release);
        }
        #[cfg(test)]
        self.audit();
    }

    /// The marks taken to visit at a writing, as [`super::keyed::Readied::begin_visits`]: how
    /// many.
    pub(super) fn begin_visits(&mut self) -> usize {
        let visits = self.replies.begin_visits();
        #[cfg(test)]
        self.audit();
        visits
    }

    /// The next reply to visit, at its slot, as [`super::keyed::Readied::next_visit`].
    pub(super) fn next_visit(&mut self) -> Option<(K, usize)> {
        let exchange = self.replies.next_visit()?;
        let at = self.replies.slot(exchange)?;
        Some((exchange, at))
    }
}

/// The node's task's half of each native put's body in flight (`Node::feeding`), each found by its
/// exchange in one probe and marked to visit at each event that can let it move
/// (`keyed::Readied`): bytes on its exchange, its job taking a piece or letting go of its body
/// ([`Native::mark`]). Its exchanges' type a parameter only so a test counts it with a stand-in
/// for `ExchangeId`, as [`Writes`]'s. Bound: one a native put the host runs.
pub(super) struct Feedings<K = ExchangeId>
where
    (K, Feeding): Key<Of = K>,
{
    feeding: super::keyed::Readied<(K, Feeding)>,
}

impl<K> Default for Feedings<K>
where
    (K, Feeding): Key<Of = K>,
{
    /// None, which allocates nothing (`Readied::default`).
    fn default() -> Self {
        Self {
            feeding: super::keyed::Readied::default(),
        }
    }
}

impl<K: Copy> Feedings<K>
where
    (K, Feeding): Key<Of = K>,
{
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.feeding.len()
    }

    pub(super) fn contains(&self, exchange: K) -> bool {
        self.feeding.contains(exchange)
    }

    /// Holds `feeding`, last, marked, as [`super::keyed::Readied::push`].
    pub(super) fn push(&mut self, feeding: (K, Feeding)) -> Option<(K, Feeding)> {
        let displaced = self.feeding.push(feeding);
        #[cfg(test)]
        self.audit();
        displaced
    }

    /// The feeding on `exchange`, to change all but what tells it moved: what its job shares.
    pub(super) fn get_mut(&mut self, exchange: K) -> Option<&mut (K, Feeding)> {
        self.feeding.get_mut(exchange)
    }

    pub(super) fn at_mut(&mut self, at: usize) -> Option<&mut (K, Feeding)> {
        self.feeding.at_mut(at)
    }

    pub(super) fn take(&mut self, at: usize) -> Option<(K, Feeding)> {
        let feeding = self.feeding.take(at);
        #[cfg(test)]
        self.audit();
        feeding
    }

    pub(super) fn remove(&mut self, exchange: K) -> Option<(K, Feeding)> {
        let at = self.feeding.slot(exchange)?;
        self.take(at)
    }

    /// Marks the feeding on `exchange` to visit at the next feeding: something it waits on came.
    pub(super) fn mark(&mut self, exchange: K) {
        self.feeding.mark(exchange);
        #[cfg(test)]
        self.audit();
    }

    /// Marks every feeding: a mark found no room ([`Marks::missed`]).
    pub(super) fn mark_all(&mut self) {
        self.feeding.mark_all();
        #[cfg(test)]
        self.audit();
    }

    /// The marks taken to visit at a feeding, as [`super::keyed::Readied::begin_visits`].
    pub(super) fn begin_visits(&mut self) -> usize {
        let visits = self.feeding.begin_visits();
        #[cfg(test)]
        self.audit();
        visits
    }

    /// The next feeding to visit, at its slot, as [`super::keyed::Readied::next_visit`].
    pub(super) fn next_visit(&mut self) -> Option<usize> {
        let exchange = self.feeding.next_visit()?;
        self.feeding.slot(exchange)
    }
}

/// The node's marks (node.md §2.9 step 6c): the exchanges whose native job gave its door something
/// the node's task acts on since the node's task took the last, each named once by its job
/// ([`Native::mark`]) on a bounded channel from whichever shard it runs on, taken at each turn
/// before the replies and feedings are visited (`Node::take_marks`); and the word that a mark
/// found no room, on which every reply and feeding is visited. Bound: the channel's room, twice
/// the exchanges the transport holds at once ([`Marks::new`]), each named once while its door's
/// word stands (`Handed::marked`); none made where the channel cannot be, on which every turn
/// visits each.
pub(super) struct Marks {
    taken: Option<(ChannelReceiver<ExchangeId>, Marker)>,
    room: usize,
}

/// A door's end of the node's marks: the channel's sender, whose clone is a count
/// (`Sender::clone`), and the word a mark with no room sets.
#[derive(Clone)]
pub(super) struct Marker {
    marks: Sender<ExchangeId>,
    missed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Marks {
    /// Marks with room for twice `exchanges`, the exchanges the transport holds, at least one: a
    /// door names its exchange once until the node's task takes the name, and once more where it
    /// is given back and taken by a new exchange first, its old name waiting, which the node's
    /// task passes over. A door given back and taken again more often within a turn can find no
    /// room, which visits every reply and feeding.
    pub(super) fn new(exchanges: usize) -> Self {
        let room = exchanges.saturating_mul(2).max(1);
        let taken = hyper_rt::sync::channel(room).ok().map(|(marks, taken)| {
            (
                taken,
                Marker {
                    marks,
                    missed: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                },
            )
        });
        Self { taken, room }
    }

    /// The end a door names its exchange on, kept with the door.
    fn marker(&self) -> Option<Marker> {
        self.taken.as_ref().map(|(_, marker)| marker.clone())
    }

    /// Whether a mark found no room since the last call, or no channel was made: every reply and
    /// feeding is visited.
    pub(super) fn missed(&self) -> bool {
        match &self.taken {
            Some((_, marker)) => marker
                .missed
                .swap(false, std::sync::atomic::Ordering::AcqRel),
            None => true,
        }
    }

    /// The marks' room, the most taken at a turn.
    pub(super) fn room(&self) -> usize {
        self.room
    }

    /// The next exchange named, in the order named.
    pub(super) fn take(&mut self) -> Option<ExchangeId> {
        let (taken, _) = self.taken.as_mut()?;
        taken.try_recv().ok().flatten()
    }
}

/// A host's calls (node.md §2.9 step 6d): the native jobs whose door the node's task moved since
/// the host's step took the last, each named by its name (`Native::name`) as the node's task gives
/// a piece of its body or lets go of it, takes or writes whole an answer of its reply, or lets go
/// of its reply ([`Caller`]), so the step wakes those jobs alone (`Host::take_calls`), where it
/// stepped every native job at every turn; and the word that a name found no room, on which every
/// native job is woken. The S3 door's names are the same rule for its connections (`door::Mark`).
/// Bound: the channel's room, twice the exchanges the host's jobs come on at once
/// ([`Calls::new`]), each job named once while its door's word stands (`Handed::called`); none
/// made where the channel cannot be, on which no door names its job and none rests on its door.
pub(crate) struct Calls {
    taken: ChannelReceiver<u64>,
    caller: std::sync::Arc<Caller>,
    room: usize,
}

/// The node's task's end of a host's calls, one a host, shared by the doors of the jobs it runs:
/// the channel's sender, the word a name with no room sets, and, for a driving shard's host, that
/// shard's word that a door of its jobs moved, which its task waits on (`driving::Lanes::doors`),
/// so one pointer a door both names its job and wakes the task that steps it.
pub(crate) struct Caller {
    calls: Sender<u64>,
    missed: std::sync::atomic::AtomicBool,
    driving: Option<std::sync::Arc<hyper_rt::sync::Notify>>,
}

impl Calls {
    /// Calls with room for twice `exchanges`, the exchanges the host's jobs come on at once, at
    /// least one: a job is named once until its host takes the name, and once more where its door
    /// is kept and taken by a new request first, its old name waiting, which the host passes
    /// over, as the node's marks ([`Marks::new`]); `driving` the word of the driving shard whose
    /// host takes them. None where the channel cannot be made.
    pub(crate) fn new(
        exchanges: usize,
        driving: Option<std::sync::Arc<hyper_rt::sync::Notify>>,
    ) -> Option<Self> {
        let room = exchanges.saturating_mul(2).max(1);
        let (calls, taken) = hyper_rt::sync::channel(room).ok()?;
        Some(Self {
            taken,
            caller: std::sync::Arc::new(Caller {
                calls,
                missed: std::sync::atomic::AtomicBool::new(false),
                driving,
            }),
            room,
        })
    }

    /// The node's task's end.
    pub(crate) fn caller(&self) -> std::sync::Arc<Caller> {
        std::sync::Arc::clone(&self.caller)
    }
}

impl Caller {
    /// Names the job whose door `handed` is, once until its host takes the name (`handed`'s
    /// word), a name with no room setting the word on which the host wakes every native job, and
    /// wakes the driving shard's task where one runs the job.
    fn name(&self, handed: &Handed) {
        if !handed
            .called
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            let name = handed.name.load(std::sync::atomic::Ordering::Acquire);
            if self.calls.try_send(name).is_err() {
                self.missed
                    .store(true, std::sync::atomic::Ordering::Release);
            }
        }
        if let Some(driving) = &self.driving {
            driving.notify_one();
        }
    }
}

/// The beat a reply waiting for room is vouched for by, half of it apart (`vouch_every`): the
/// probe timeout before any sample (`bounds::initial_pto`, RFC 9002 §6.2.1), a constant.
fn room_beat() -> u64 {
    u64::try_from(bounds::initial_pto().as_nanos()).unwrap_or(u64::MAX)
}

/// A request's body as its door gives it: its pieces on a bounded channel, and its end once it
/// came, with the checksum its trailer carried.
struct Pieces {
    pieces: ChannelReceiver<Piece>,
    ended: Option<Option<checksum::Checksum>>,
    /// Whether its channel held no piece as its put last looked (`take_body`): the put waits on
    /// its door for more.
    dry: bool,
}

/// A local door's ends: the request's body as the listener sends it, and the reply as the host
/// writes it.
struct Local {
    /// The body, until the job answers: let go of then (`Host::drain_job`), its channel closed, so
    /// a connection still sending it stops and reads the reply.
    body: Option<Pieces>,
    reply: Sender<door::Answer>,
    /// Answers the reply's channel had no room for, sent before any other, in order. A stream's
    /// next piece is taken only once these are sent, so they are at most one piece of bytes and
    /// the few answers around it.
    pending: VecDeque<door::Answer>,
    /// Whether the listener was told the put wants its body.
    ready: bool,
    /// The request's name, which its connection names as it gives the job a piece of its body or
    /// room in its reply, or lets go of it (`door::Mark`): the job's turns pass it by while it
    /// waits on its connection alone (`Job::rests`). None for a job made with none.
    mark: Option<std::sync::Arc<door::Mark>>,
}

/// A native put's body as the node's task reads it from its exchange for the put's job (node.md
/// §2.9 step 6c), the exchange's half of what the S3 listener does for a local door: each piece
/// the transport delivers given on the job's channel as an S3 connection gives one
/// (`door::Piece`), the object's bytes apart from the trailer's value past them, and the body's
/// end once the transport verified the body whole, the trailer's checksum with it. A piece is read
/// only once the job let go of the last, so the channel holds one piece and the exchange's stream
/// the rest: while the job's driver takes nothing, nothing is read, and the stream's flow control
/// holds the client, as it did while the job read its own exchange.
///
/// Each piece is read into the node's task's piece (`Node::piece`, never the endpoint's budget,
/// which other traffic may hold past the client's patience) and copied once from it into storage
/// of the feeding's own, the piece the put's admission charged it for, that the piece is a view of
/// (`Bytes`) and that is reused once the job let it go: a piece is copied no more than when its job
/// read it, and in storage reused from piece to piece.
pub(crate) struct Feeding {
    /// The body's sender, lent by the put's door ([`Replies`]) and given back as the feeding is let
    /// go ([`Feeding::let_go`]), so no channel is made a put.
    pieces: Sender<Piece>,
    /// What the put's door's halves share: whether its job let go of its body, and where the
    /// feeding is let go, that the node's task did.
    handed: std::sync::Arc<Handed>,
    /// The storage the piece in flight is a view of, made at the first piece, and the bytes it was
    /// made to hold: what was left of the object then, at most a piece.
    storage: Option<(BytesMut, usize)>,
    /// A piece or end the channel had no room for, given before anything more is read.
    held: Option<Piece>,
    /// The object's bytes still to come, the trailer's algorithm, and its value read so far.
    left: u64,
    trailing: Option<checksum::Algorithm>,
    trailer: Vec<u8>,
    /// Whether the put's body is empty, its end given with no read of the exchange, as the job
    /// ended it when it read its exchange.
    empty: bool,
    /// Whether the job let go of the body: the rest is read and dropped, so its exchange ends whole
    /// for its reply.
    dropping: bool,
    /// Its door's caller (`Replies::call`), told as a piece or the end is given or the feeding is
    /// let go, so the job takes it.
    call: Option<std::sync::Arc<Caller>>,
}

/// What became of a body the node's task fed at a turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fed {
    /// Nothing moved: the job holds the last piece, or no bytes arrived.
    Still,
    /// A body answered at once waits for room in the endpoint's budget for the piece it drains
    /// through (`Node::drop_body`).
    Room,
    /// A piece or the end given, or bytes read and dropped: the job's next turn is due at once.
    Moved,
    /// The body is ended, given or dropped whole, or its exchange failed.
    Over,
}

impl Feeding {
    /// The bytes `read` from the exchange, after the fields: the object's given to the job as a
    /// piece, which counts them moved as it takes them, and the trailer's value past them kept for
    /// the end, counted moved here. Refused `Malformed` where they pass the trailer's width, which
    /// the transport's length of the body rules out (`Head::decode`).
    fn give(&mut self, read: &[u8], moved: &mut u64) -> Result<Fed, Status> {
        let next = self.next_piece();
        let object = next.min(read.len());
        let (bytes, trailer) = read.split_at_checked(object).ok_or(Status::Failed)?;
        let width = self.trailing.map_or(0, checksum::Algorithm::width);
        if self.trailer.len().saturating_add(trailer.len()) > width {
            return Err(Status::Malformed);
        }
        self.trailer.extend_from_slice(trailer);
        *moved = moved.saturating_add(u64::try_from(trailer.len()).unwrap_or(u64::MAX));
        self.left = self
            .left
            .saturating_sub(u64::try_from(object).unwrap_or(u64::MAX));
        if bytes.is_empty() {
            return Ok(Fed::Still);
        }
        let (storage, _) = self
            .storage
            .get_or_insert_with(|| (BytesMut::with_capacity(next), next));
        storage.extend_from_slice(bytes);
        let piece = Piece::Bytes(
            storage.split().freeze(),
            door::Paced {
                sent: std::time::Instant::now(),
                delivered_ns: None,
            },
        );
        Ok(self.send(piece))
    }

    /// The object's bytes the next piece may hold: what is left of them, at most a piece. The
    /// storage is made this large at the first piece, so a body whose last bytes come apart from
    /// its fields' read holds no more than they.
    fn next_piece(&self) -> usize {
        usize::try_from(self.left)
            .unwrap_or(usize::MAX)
            .min(piece_len())
    }

    /// Whether the job let go of the last piece, so the next may be read into its storage: none
    /// made yet, or reclaimed whole, every view of it dropped. Whole, not only the room the next
    /// piece needs, which a short piece leaves past it while the job still holds it.
    fn free(&mut self) -> bool {
        self.held.is_none()
            && self
                .storage
                .as_mut()
                .is_none_or(|(storage, made)| storage.try_reclaim(*made))
    }

    /// Whether the object's bytes and the trailer's value are all read.
    fn read_whole(&self) -> bool {
        self.left == 0 && self.trailer.len() == self.trailing.map_or(0, checksum::Algorithm::width)
    }

    /// The body's end, its trailer's checksum with it: `Over` once given, or once the job let go
    /// of a body the transport already verified whole; held where the channel has no room.
    fn end(&mut self) -> Fed {
        let trailer = self.trailing.map(|algorithm| checksum::Checksum {
            algorithm,
            bytes: std::mem::take(&mut self.trailer),
        });
        match self.send(Piece::End(trailer)) {
            Fed::Still | Fed::Room => Fed::Still,
            Fed::Moved | Fed::Over => Fed::Over,
        }
    }

    /// Gives `piece` to the job, or holds it where the channel has no room; a job that let go of
    /// its body has the rest dropped.
    fn send(&mut self, piece: Piece) -> Fed {
        if self.job_let_go() {
            self.held = None;
            self.dropping = true;
            return Fed::Moved;
        }
        match self.pieces.try_send(piece) {
            Ok(()) => {
                self.wake_job();
                Fed::Moved
            }
            Err(SyncError::Full(piece)) => {
                self.held = Some(piece);
                Fed::Still
            }
            Err(SyncError::Closed(_) | SyncError::NotOnShardThread(_)) => {
                self.dropping = true;
                Fed::Moved
            }
        }
    }

    /// Whether the put's job let go of its body, or of its door: the rest is read and dropped, a
    /// piece it never took holding the storage until its door is kept, emptied ([`Doors::keep`]).
    fn job_let_go(&self) -> bool {
        [&self.handed.dropped, &self.handed.ended]
            .into_iter()
            .any(|word| word.load(std::sync::atomic::Ordering::Acquire))
    }

    /// The feeding let go, its body given whole or dropped, or its exchange gone: its door's body
    /// sender given back, and its job told the node's task let go of the body (`Handed::cut`),
    /// which a job that took the body's end never reads.
    fn let_go(self) -> Sender<Piece> {
        self.handed
            .cut
            .store(true, std::sync::atomic::Ordering::Release);
        self.wake_job();
        self.pieces
    }

    /// Names its job to the host that runs it and wakes the driving shard it runs on, where one
    /// does: its body moved.
    fn wake_job(&self) {
        if let Some(call) = &self.call {
            call.name(&self.handed);
        }
    }

    /// Gives what the channel had no room for, first: whether it went, and whether it was the end.
    fn send_held(&mut self) -> Option<Fed> {
        let held = self.held.take()?;
        let end = matches!(held, Piece::End(_));
        Some(match self.send(held) {
            Fed::Moved if end && !self.dropping => Fed::Over,
            fed => fed,
        })
    }
}

/// The body a native put's operation names, its object's length and its trailer's algorithm, which
/// its feeding reads (`feed`): a put's, a part's or a run's, the operations whose work is a put.
fn body_of(op: &Op) -> Option<(u64, Option<checksum::Algorithm>)> {
    match op {
        Op::Put {
            length, trailing, ..
        }
        | Op::UploadPart {
            length, trailing, ..
        }
        | Op::Append {
            length, trailing, ..
        } => Some((*length, *trailing)),
        _ => None,
    }
}

/// A native put, its fields read and nothing past them, given its body by the node's task (node.md
/// §2.9 step 6c): of its object's length and its trailer's algorithm, from its first byte on its
/// door's channel of one piece, the least that moves a body (`door::Depth::FIRST`), `pieces` the
/// sender its door lent and `handed` what its door's halves share, from the feeding, which the node's task keeps, to its door's receiver, which the
/// put's job takes from, the trailer's value past the object's bytes the feeding's; `call` the
/// caller of the host that runs its job ([`Caller`]), none where none is told.
fn feed(
    (length, trailing): (u64, Option<checksum::Algorithm>),
    pieces: Sender<Piece>,
    handed: std::sync::Arc<Handed>,
    call: Option<std::sync::Arc<Caller>>,
) -> Feeding {
    let width = trailing.map_or(0, checksum::Algorithm::width);
    Feeding {
        pieces,
        handed,
        storage: None,
        held: None,
        left: length,
        trailing,
        trailer: Vec::new(),
        empty: length == 0 && width == 0,
        dropping: false,
        call,
    }
}

/// The bytes of a native body's piece: the transport's receive chunk (`PIECE`).
fn piece_len() -> usize {
    usize::try_from(PIECE).unwrap_or(usize::MAX)
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

/// Sends what waits on `reply`, in order: whether it takes more now.
fn flush<T>(reply: &Sender<T>, pending: &mut VecDeque<T>) -> Sent {
    while let Some(answer) = pending.pop_front() {
        match reply.try_send(answer) {
            Ok(()) => {}
            Err(SyncError::Full(answer)) => {
                pending.push_front(answer);
                return Sent::Held;
            }
            Err(SyncError::Closed(_) | SyncError::NotOnShardThread(_)) => return Sent::Gone,
        }
    }
    Sent::Taken
}

impl Local {
    /// Sends what waits, in order: whether the door takes more now.
    fn flush(&mut self) -> Sent {
        flush(&self.reply, &mut self.pending)
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
    /// A native request's fields read, its operation and its Open's block to be handed to the
    /// driving shard it was placed on (node.md §2.9 step 6e), which makes its work there: taken
    /// out by the node's task after the host's step (`Node::hand_off`).
    Handing(Box<(Op, Option<u64>)>),
    /// A request refused or done: its reply, which goes once what is left of its body is read and
    /// dropped, so the exchange ends whole (`Arriving::Redirect`'s reason). Boxed: a reply is many
    /// times any other job's state.
    Draining(Box<Reply>),
    /// An operation's last answers going out, in order: a local one's its reply and its end, or
    /// its end; a native one's wait in its door.
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
    /// DeleteObjects: its bucket's row read, then each object's delete (research/89 §5).
    Deletes(Box<Deleting>),
    /// CopyObject or UploadPartCopy: the source read by a `Get`, written by a `Put` (research/89
    /// §3–§4).
    Copy(Box<Copying>),
    /// An operator's request on the account's principals and their keys.
    Admin(Box<Administering>),
}

/// The version a request's `versionId` names: `null`, or one of mantle's 13-character IDs
/// (`mantle_meta::key::version_id`); `InvalidVersion` for any other text, S3's `400
/// InvalidArgument`, "Invalid version id specified" (research/89 §2.1).
fn named(version: Option<&str>) -> Result<Option<Named>, Status> {
    match version {
        None => Ok(None),
        Some(NULL_VERSION) => Ok(Some(Named::Null)),
        Some(id) => parse_version_id(id)
            .map(|order| Some(Named::Order(order)))
            .ok_or(Status::InvalidVersion),
    }
}

/// A DeleteObjects: its objects, each one's delete asked in order, at most the commands an
/// operation is given out at once, and each one's result in its place (research/89 §5).
pub(crate) struct Deleting {
    bucket: String,
    objects: Vec<Target>,
    quiet: bool,
    /// The bucket's incarnation and versioning, once its row is read.
    row: Option<(u64, Versioning)>,
    /// The first object not yet asked.
    next: usize,
    /// Deletes asked and not yet answered.
    out: usize,
    /// The node had no room for its next delete at its last turn: asked again at the next.
    held: bool,
    results: Vec<Option<Deleted>>,
}

impl Deleting {
    /// The objects at `denied`, which its principal's policy does not let it delete, each
    /// answered `AccessDenied` in its place, as S3 answers them, its delete never asked
    /// (research/91 §6).
    fn deny(&mut self, denied: &[usize]) {
        for &at in denied {
            let (Some(target), Some(slot)) = (self.objects.get(at), self.results.get_mut(at))
            else {
                continue;
            };
            *slot = Some(Deleted {
                key: target.key.clone(),
                version: target.version.clone(),
                status: Status::AccessDenied,
                marker: None,
            });
        }
    }
}

/// Where a copy is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyStage {
    /// The destination bucket's row to read.
    Bucket,
    /// An UploadPartCopy's upload row to read, for its checksum's algorithm.
    Upload,
    /// The source's version to read.
    Source,
    /// The source read by a `Get` and written by a `Put`.
    Run,
    /// CopyObject's version committed as its key's null version: its mark read for the time the
    /// range gave it (research/89 §3.6).
    Mark,
}

/// A CopyObject or an UploadPartCopy (research/89 §3–§4).
pub(crate) struct Copying {
    bucket: String,
    key: String,
    /// UploadPartCopy's upload and part number; CopyObject's none.
    part: Option<(String, u16)>,
    id: u128,
    source: (String, String, Option<Named>),
    /// `x-amz-copy-source-if-*`, and a part's `x-amz-copy-source-range`.
    conditions: Headers,
    /// The destination's `If-Match` and `If-None-Match`.
    preconditions: Preconditions,
    directive: Directive,
    metadata: Metadata,
    checksum: Option<Algorithm>,
    stage: CopyStage,
    /// The destination bucket's row, and an UploadPartCopy's upload's.
    row: Option<Bucket>,
    upload: Option<Upload>,
    get: Option<Get>,
    put: Option<Put>,
    /// What the put checks at its end: the source's own MD5 and checksum, where it copies the
    /// whole source and the source keeps them.
    expected: Expected,
    /// Plaintext the get gave and the put has not yet taken, and how much of it the put took.
    carry: Option<(Bytes, usize)>,
    /// The put's body ended.
    ended: bool,
    get_waiting: VecDeque<(u64, get::Request)>,
    put_waiting: VecDeque<(u64, put::Request)>,
    /// The source version's ID.
    source_version: String,
    /// The put's file, whose mark names the version a null version's copy made.
    file: u128,
    /// The put's outcome, while the version's time is read.
    stored: Option<put::Stored>,
}

/// Which of a copy's drivers a request of its job is for: its ask's request ID carries it in its
/// lowest bit, the driver's own ID above it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Driver {
    Get,
    Put,
}

/// The request ID a copy's ask is kept under: the driver's ID shifted past a bit naming the
/// driver; none past what a u64 holds, a driver's IDs counting its requests from zero.
fn tagged(driver: Driver, id: u64) -> Option<u64> {
    let bit = match driver {
        Driver::Get => 0,
        Driver::Put => 1,
    };
    id.checked_mul(2)?.checked_add(bit)
}

fn untagged(tag: u64) -> (Driver, u64) {
    let driver = if tag & 1 == 0 {
        Driver::Get
    } else {
        Driver::Put
    };
    (driver, tag >> 1)
}

/// An operator's request on the account's principals and their keys (docs/design/principals.md):
/// one command of the root range, or one read of it, asked once and replied from its answer. Its
/// asker is the account's root, as every client of the native protocol is (node.md §3.6).
pub(crate) struct Administering {
    asking: Admin,
    asked: bool,
    /// A key made: its id and its secret, held until the root range answers and replied once.
    issued: Option<(String, mantle_s3::account::Secret)>,
}

/// What an operator's request asks of the root range.
enum Admin {
    Command(Box<root::Command>),
    /// A page of principals, or with `one` the principal asked, whose absence is `NoSuchEntity`.
    Read {
        query: Query,
        one: bool,
    },
    /// A key for `principal`: its id and secret drawn and the secret wrapped under the root key's
    /// current generation at its first turn, then asked as a command.
    Key {
        principal: String,
        rotating: bool,
    },
}

/// A principal's key a connection of the S3 listener asked for (`door::KeyAsk`): read from the
/// root range by ReadIndex, a read that began after the request came, so a key revoked by a
/// command committed before it is refused at every node; its secret unwrapped under the root
/// key, and answered on its own channel (docs/design/principals.md).
pub(crate) struct Lookup {
    id: String,
    reply: Sender<door::Signed>,
    /// The read asked, once the node had room for it.
    ask: Option<ReadAsk>,
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
    /// the listener's door, or the value past a native put's bytes (`object::Op::Put`), which the
    /// node's task reads ([`Feeding`]).
    trailing: Option<checksum::Algorithm>,
    /// The driver, once the bucket's row was read.
    put: Option<Put>,
    /// Body bytes read and not yet taken by the driver, and the body's bytes taken so far.
    carry: Vec<u8>,
    taken: u64,
    /// The driver's requests not yet asked: the node held as many asks as it keeps.
    waiting: VecDeque<(u64, put::Request)>,
    /// A native upload's run (an Append): its upload's row read after its bucket's.
    run: Option<Appending>,
    /// A PutObject's content headers and user-defined metadata, which its version keeps
    /// (research/89 §1).
    metadata: Metadata,
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
    /// The version `versionId` names; none for the key's current one (research/89 §2.1).
    version: Option<Named>,
    /// A HeadObject: the version's facts, and no bytes.
    head: bool,
    get: Option<Get>,
    /// The reply, held until the driver has the range's first bytes or its end, so a block that
    /// cannot be read before any byte goes is answered with a typed refusal rather than a reply
    /// begun and cut (docs/design/repair.md §6); none once sent.
    held: Option<Box<Reply>>,
    /// Bytes of the range given to the door so far, and the range's length.
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
    /// The bucket's row, read and kept while the node has no room for the command made from it:
    /// made and asked at a later turn, as a completion's and a put's requests wait, so nothing of
    /// the request is spent on an ask the node refuses.
    row: Option<Bucket>,
}

/// The command a bucket's row is read for.
enum Then {
    /// DeleteObject: the Name range's delete (05 §7.3), of the version `versionId` names where
    /// it names one (research/89 §2.2).
    Delete {
        key: String,
        if_match: Option<Match>,
        named: Option<Named>,
    },
    /// PutBucketVersioning, by the bucket's owner (05 §7.1).
    Versioning(Versioning),
    /// CreateMultipartUpload, with the checksum its parts carry (05 §4.2) and the metadata its
    /// completed version keeps (research/89 §1).
    CreateUpload {
        key: String,
        checksum: Option<(Algorithm, bool)>,
        metadata: Metadata,
    },
    /// A native upload's Open (gateway.md §2.1): its object's length where stated, its
    /// full-object checksum's algorithm, the length of its runs (`name::run_length`) and its
    /// object's metadata.
    Open {
        key: String,
        length: Option<u64>,
        checksum: Algorithm,
        run: u64,
        metadata: Metadata,
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
    /// ListObjectVersions: a page of the bucket's versions, its query made from the request.
    Versions(Query),
}

/// The volume table as this node's member of the root range held it after an entry it applied,
/// read again once per entry the member applies, the only thing that changes it; and the scheme
/// new blocks take over it, chosen again only when the table's failure domains changed, the one
/// thing of the table the choice reads (the gateway's rates and target are the process's own).
pub(crate) struct Table {
    applied: u64,
    /// In the order of their ids (`root::volumes`).
    placed: Vec<Placed>,
    /// The same volumes, each with its standing and its node's presence (`root::facts`): what
    /// placement and repair judge by (docs/design/repair.md §2).
    pub(super) facts: Vec<root::Fact>,
    /// The volumes that take chunks now, which the scheme is chosen over.
    standing: Vec<Placed>,
    scheming: Scheming,
}

impl Table {
    /// The table of `facts`, read as of entry `applied`, after `was`: its scheme carried where the
    /// domains of the volumes that take chunks now are as they were, and asked again where they
    /// changed (`Scheming::settle`, which `Node::table` calls at each look).
    fn of(was: Option<Table>, applied: u64, facts: Vec<root::Fact>) -> Self {
        let placed: Vec<Placed> = facts.iter().map(|f| f.placed.clone()).collect();
        let standing: Vec<Placed> = facts
            .iter()
            .filter(|f| f.takes())
            .map(|f| f.placed.clone())
            .collect();
        let same = was
            .as_ref()
            .is_some_and(|t| same_domains(&t.standing, &standing));
        let scheming = Scheming::after(was.map(|t| t.scheming), same, applied);
        Self {
            applied,
            placed,
            facts,
            standing,
            scheming,
        }
    }

    /// The node that holds `volume`, if the table holds it.
    pub(super) fn node_of(&self, volume: u128) -> Option<PeerId> {
        let at = self
            .placed
            .binary_search_by_key(&volume, |p| p.volume)
            .ok()?;
        self.placed.get(at).map(|p| p.node)
    }

    /// The root range's applied index the table was read at.
    pub(super) fn applied(&self) -> u64 {
        self.applied
    }

    /// A settled copy for a driving shard (node.md §2.9 step 6e): its volumes and the scheme new
    /// blocks take now, no choice running, as the node's task alone asks the durability model.
    pub(super) fn settled(&self) -> Self {
        Self {
            applied: self.applied,
            placed: self.placed.clone(),
            facts: self.facts.clone(),
            standing: self.standing.clone(),
            scheming: Scheming {
                now: self.scheming.now,
                domains_at: self.scheming.domains_at,
                settled: true,
                choosing: None,
            },
        }
    }

    /// The scheme new blocks take now: none where no candidate fits the domains, and refused
    /// where the durability model's choice for them unwound on the compute pool, until the
    /// domains change and it is asked again.
    pub(super) fn scheme(&self) -> Result<Option<durability::Scheme>, NodeError> {
        self.scheming.now.map_err(|Unwound| {
            NodeError::Runtime("the durability model's choice unwound on the compute pool".into())
        })
    }
}

/// The durability model's choice for a table's domains, and the scheme new blocks take meanwhile
/// (docs/design/node.md §5.1). The model runs on hyper-rt's compute pool, off the shard, which it
/// held for 2,448.5 allocations each time it ran there (research/71); the shard reads the answer
/// once it landed. Until then new blocks take the scheme chosen before wherever it still fits the
/// domains, no wider than they are: the model prices a scheme by the scheme and the gateway's
/// rates alone, the domains only ruling out the wider candidates (durability.md §4), so a block
/// placed under it, one chunk to each domain (`place::offer`), is exactly as durable as the blocks
/// before the change: within the target, or as near it as the cell came. Where it no longer fits,
/// or none was chosen yet, a block placed under it would put two of its chunks in one domain,
/// which the model never priced: placements wait for the choice, made at once on the shard.
struct Scheming {
    /// The scheme new blocks take now.
    now: Result<Option<durability::Scheme>, Unwound>,
    /// The generation of the table's domains: the applied index of the reading at which they last
    /// changed. A choice is asked for one and its answer taken only for the same.
    domains_at: u64,
    /// Whether `now` was chosen for the domains of `domains_at`.
    settled: bool,
    /// The choice running on the compute pool, at most one: a table whose domains changed again
    /// while it ran asks again once its stale answer lands, as a per-node task is discarded if its
    /// node changed meanwhile (CLAUDE.md §10).
    choosing: Option<Choosing>,
}

/// The durability model's choice for a table's domains unwound on the compute pool: its job's
/// receiver saw the sender gone without a value (hyper-rt `compute::run`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Unwound;

/// A choice running on the compute pool: the generation of the domains it was asked for, and its
/// answer.
struct Choosing {
    asked_at: u64,
    answer: OneshotReceiver<Option<durability::Scheme>>,
}

impl Scheming {
    /// The scheming of a table read at `applied`, after `was`'s: carried whole where the domains
    /// are the `same`, and otherwise a new generation, the scheme chosen before kept until
    /// `settle` judges whether it fits, and a choice still running kept to be judged stale.
    fn after(was: Option<Scheming>, same: bool, applied: u64) -> Self {
        match was {
            Some(was) if same => was,
            Some(was) => Self {
                now: was.now,
                domains_at: applied,
                settled: false,
                choosing: was.choosing,
            },
            None => Self {
                now: Ok(None),
                domains_at: applied,
                settled: false,
                choosing: None,
            },
        }
    }

    /// Takes the running choice's answer if it landed, and asks the model where the scheme is not
    /// yet chosen for the domains of the volumes that take chunks now, `standing`: on the compute
    /// pool while the scheme held fits them, and on the shard at once where it does not, or where
    /// the pool refuses the job (none in this process, one at its bound, or one stopping: the
    /// choice is never left undone, and runs where it ran before the pool).
    fn settle(&mut self, gateway: Option<&GatewaySettings>, standing: &[Placed]) {
        self.take();
        let asked = self
            .choosing
            .as_ref()
            .is_some_and(|c| c.asked_at == self.domains_at);
        if self.settled || asked {
            return;
        }
        let Some(gateway) = gateway else {
            return;
        };
        let ask = Ask::of(gateway, standing);
        let fits = matches!(self.now, Ok(Some(scheme)) if scheme.width() <= ask.domains);
        if fits && self.choosing.is_some() {
            return;
        }
        if fits {
            match compute::run(move || ask.choose()) {
                Ok(answer) => {
                    self.choosing = Some(Choosing {
                        asked_at: self.domains_at,
                        answer,
                    });
                    return;
                }
                Err(refused) => {
                    let job = refused.job;
                    self.now = Ok(job());
                }
            }
        } else {
            self.now = Ok(ask.choose());
        }
        self.settled = true;
    }

    /// The running choice's answer, if it landed: taken where it was asked for the domains the
    /// table has, and dropped where they changed since.
    fn take(&mut self) {
        let Some(choosing) = self.choosing.as_mut() else {
            return;
        };
        let landed = match choosing.answer.try_recv() {
            Ok(None) => return,
            Ok(Some(scheme)) => Ok(scheme),
            Err(_) => Err(Unwound),
        };
        let current = choosing.asked_at == self.domains_at;
        self.choosing = None;
        if current {
            self.now = landed;
            self.settled = true;
        }
    }
}

/// What the durability model's choice reads, owned, so it runs on the compute pool: the
/// candidates, the count of the domains that take chunks now, and the gateway's rates and target.
struct Ask {
    candidates: Vec<durability::Scheme>,
    domains: usize,
    rates: Rates,
    target: f64,
}

impl Ask {
    /// The choice over `standing`: a domain counts while it holds a volume that takes chunks now,
    /// active and its node seen.
    fn of(gateway: &GatewaySettings, standing: &[Placed]) -> Self {
        let mut domains: Vec<&str> = standing.iter().map(|p| p.domain.as_str()).collect();
        domains.sort_unstable();
        domains.dedup();
        Self {
            candidates: durability::candidates(),
            domains: domains.len(),
            rates: gateway.rates.clone(),
            target: gateway.target,
        }
    }

    /// The scheme a new block takes: the durability model's choice among the schemes that fit the
    /// cell's failure domains, or the closest to the target where none meets it, as a cell short
    /// of domains places on those there are (node.md §1.5). A block written while machines sleep
    /// takes the scheme the machines seen meet the target with, a chunk to each domain, rather
    /// than waiting for them or doubling chunks on the domains there are (docs/design/repair.md
    /// §5).
    fn choose(self) -> Option<durability::Scheme> {
        match durability::choose(&self.candidates, self.domains, &self.rates, self.target) {
            Ok(Some(Choice::Meets { scheme, .. } | Choice::Short { scheme, .. })) => Some(scheme),
            _ => None,
        }
    }
}

/// Whether two readings of the volume table hold the same volumes in the same failure domains.
fn same_domains(was: &[Placed], now: &[Placed]) -> bool {
    was.len() == now.len()
        && was
            .iter()
            .zip(now)
            .all(|(a, b)| a.volume == b.volume && a.domain == b.domain)
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

/// What one object's delete came to (research/89 §5.2): done, with the delete marker it made or
/// removed, or the refusal its Name range's answer earns.
fn deleted(target: &Target, outcome: name::Outcome) -> Deleted {
    let (status, marker) = match outcome {
        // "When a multi-object Delete request results in Amazon S3 either adding a delete marker
        // or removing a delete marker, the response returns" the marker's version ID.
        name::Outcome::Deleted { marker, version } => {
            (Status::Done, if marker { version } else { None })
        }
        refused => (name_status(&refused), None),
    };
    Deleted {
        key: target.key.clone(),
        version: target.version.clone(),
        status,
        marker,
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

/// The reply a put's failure is answered with (node.md §4.4): a block no volume offered took,
/// `SlowDown` for that cause.
fn put_failed(error: &PutError) -> Reply {
    Reply::status(match error {
        PutError::IncompleteBody => Status::IncompleteBody,
        PutError::EntityTooLarge { .. } => Status::EntityTooLarge,
        PutError::BadDigest | PutError::BadChecksum => Status::BadDigest,
        PutError::Unplaced(_) => return Reply::slowed(door::Slowed::Unplaced),
        PutError::Full(_) => Status::InsufficientStorage,
        PutError::InvalidPartNumber(_) => Status::InvalidArgument,
        PutError::Name(outcome) => name_status(outcome),
        _ => Status::Failed,
    })
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

/// What an operation's driver moves, known before its fields are read: a write's body of its
/// length, an object a get reads, whose size is known only once its row is read, or neither.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Moves {
    Written(u64),
    Read,
    Neither,
}

impl Moves {
    /// What a request's head says its operation moves.
    pub(super) fn of_head(head: &Head) -> Self {
        match head.written() {
            Some(length) => Self::Written(length),
            None if head.reads() => Self::Read,
            None => Self::Neither,
        }
    }

    /// What an operation moves: a copy's get and put are neither's, both charged at once
    /// (`charged`).
    pub(super) fn of(op: &Op) -> Self {
        match (op, op.written()) {
            (_, Some(length)) => Self::Written(length),
            (Op::Get { .. }, None) => Self::Read,
            _ => Self::Neither,
        }
    }
}

/// What an op is charged once taken, and its window ([`sized`]): a copy its get's and its put's at
/// once at the floor's window (`derive::Charges::copy`), its put's body the largest a copy writes,
/// a CopyObject's or a part's (`MAX_UPLOAD`), as its source's size is known only once its version
/// is read, when its windows are fitted to it ([`copy_windows`]); any other op as what it moves
/// says. None on a node that hosts no objects.
pub(super) fn charged(
    gateway: Option<&GatewaySettings>,
    learned: &crate::window::Windows,
    op: &Op,
) -> (u64, usize) {
    match (gateway, op) {
        (Some(gateway), Op::Copy(_) | Op::CopyPart(_)) => {
            let floor = gateway.window;
            let largest = mantle_s3::body::MAX_UPLOAD;
            (gateway.charges.copy(floor, floor, largest), floor)
        }
        _ => sized(gateway, learned, Moves::of(op)),
    }
}

/// What an operation that moves `moves` holds at its own size, and the blocks its driver holds at
/// once: a write's window going down as `learned` measured the node's puts (`crate::window`), at
/// least the floor's (`GatewaySettings::window`), charged for those blocks
/// (`derive::Charges::write`); a get's at the floor's window (`Charges::read`), as its object's
/// size is known only once its row is read, widened then to the window learned of the node's gets
/// ([`reading`]); every other operation the most, at the floor's window. None on a node that
/// hosts no objects.
pub(super) fn sized(
    gateway: Option<&GatewaySettings>,
    learned: &crate::window::Windows,
    moves: Moves,
) -> (u64, usize) {
    let Some(gateway) = gateway else {
        return (0, 0);
    };
    let charges = &gateway.charges;
    match moves {
        Moves::Written(length) => {
            let learned = learned.put.blocks(length, charges.largest_block());
            let window = fitting(gateway, learned, |window| charges.write(window, length));
            (charges.write(window, length), window)
        }
        Moves::Read => (charges.read(gateway.window), gateway.window),
        Moves::Neither => (charges.of(None), gateway.window),
    }
}

/// The widest window from the floor's to the one learned whose `charge` the operations' bytes hold
/// (`gateway.objects_bytes`), the floor's where none past it does: an operation charged past them
/// would never be admitted, and the door, which takes its requests in the order they came, would
/// hold every request behind it (CLAUDE.md §2). The floor's always fits, as the operations' bytes
/// are at least the most one operation is charged (`derive::most_charged`). A charge grows with the
/// blocks its operation holds (`derive::Charges::write`, `Charges::read`), so the widest is found
/// by halving the range.
fn fitting(gateway: &GatewaySettings, learned: usize, charge: impl Fn(usize) -> u64) -> usize {
    let (mut low, mut high) = (gateway.window, learned.max(gateway.window));
    while low < high {
        let mid = low.saturating_add(high.saturating_sub(low).div_ceil(2));
        if charge(mid) <= gateway.objects_bytes {
            low = mid;
        } else {
            high = mid.saturating_sub(1);
        }
    }
    low
}

/// The blocks a get of a range of `length` bytes holds being read, admitted at `charge` for the
/// floor's window: the blocks the node learned its clients take while one is read (`learned`,
/// `crate::window`), no more than the range is cut into at the smallest block, and the widest
/// whose charge (`derive::Charges::read`) the operations' bytes hold, past what it was admitted
/// at beside the other operations' as `holds` says (`admit::Admission::holds`); the floor's where
/// none wider fits. Its object's size is known only once its row is read, so it is widened then,
/// before it holds the blocks, and takes their bytes (`Node::current_read`).
fn reading(
    gateway: &GatewaySettings,
    learned: &crate::window::Window,
    length: u64,
    charge: u64,
    holds: impl Fn(u64) -> bool,
) -> usize {
    let charges = &gateway.charges;
    let cut = length.div_ceil(charges.smallest_block().max(1));
    let most = usize::try_from(cut).unwrap_or(usize::MAX);
    let learned = learned.blocks(u64::MAX, charges.largest_block()).min(most);
    fitting(gateway, learned, |window| {
        let read = charges.read(window);
        if holds(read.saturating_sub(charge)) {
            read
        } else {
            u64::MAX
        }
    })
}

/// The blocks a copy of a span of `length` bytes holds, its get's being read and its put's going
/// down, and what it is charged for them, admitted at `charge` for the floor's windows
/// ([`charged`]): its put's the window the node learned of its puts (`crate::window`), as a put of
/// that length is ([`sized`]); then its get's beside it, as many blocks as that put drains while
/// one is read (`Windows::copy_get`), no more than the span is cut into at the smallest block;
/// each the widest whose charge (`derive::Charges::copy`) the operations' bytes hold, past what it
/// was admitted at beside the other operations' (`admit::Admission::holds`), the floor's where none
/// wider fits. The caller takes the bytes past `charge`, and gives back those the span does not
/// need, as a span shorter than the largest a copy writes holds less. Its source's size is known
/// only once its version is read, so it is fitted then, before its drivers hold the blocks
/// (`Node::copy_source_read`).
fn copy_windows(
    gateway: &GatewaySettings,
    learned: &crate::window::Windows,
    holds: impl Fn(u64) -> bool,
    length: u64,
    charge: u64,
) -> (usize, usize, u64) {
    let charges = &gateway.charges;
    let fits = |bytes: u64| {
        if holds(bytes.saturating_sub(charge)) {
            bytes
        } else {
            u64::MAX
        }
    };
    let floor = gateway.window;
    let put = learned.put.blocks(length, charges.largest_block());
    let put = fitting(gateway, put, |put| fits(charges.copy(floor, put, length)));
    let get = learned.copy_get(put, length, charges.smallest_block());
    let get = fitting(gateway, get, |get| fits(charges.copy(get, put, length)));
    (get, put, charges.copy(get, put, length))
}

/// The page caches of the ranges on the node's shard lent their parts of the operations' bytes
/// that no running operation holds once `more` are charged (`admit::cache_part`), each at most
/// `want`: a cache past its part shrinks at once (`Engine::lend_cache`), before the bytes are
/// charged and in the same turn, on the shard where both run, so the operations' charges and the
/// caches' bytes are within the operations' memory at every moment; one below its part grows,
/// keeping its pages, where `grow`, once a turn (`Node::turn`), so operations that end and begin
/// within a turn move no cache, and a part that moves by less than a page resizes none. A range
/// on another shard (`range_shards = spread`) borrows nothing: its cache is not on this shard.
pub(super) fn lend<E: Engine, S: mantle_range::LogStore>(
    want: u64,
    admission: &mut crate::admit::Admission,
    ranges: &mut [super::Kept],
    owner: &mut shard::Here<E, S>,
    more: u64,
    grow: bool,
) {
    let here = ranges
        .iter()
        .filter(|kept| owner.get(kept.handle).is_some())
        .count();
    let part = crate::admit::cache_part(admission.idle_past(more), want, here);
    let mut lent = 0u64;
    for kept in ranges.iter_mut() {
        let Some(shell) = owner.get_mut(kept.handle) else {
            continue;
        };
        let engine = shell.machine_mut().engine_mut();
        kept.lent = crate::admit::lend_to(engine, kept.lent, part, grow);
        lent = lent.saturating_add(kept.lent);
    }
    admission.lend(lent);
}

/// A bucket's versioning as GetBucketVersioning answers it: none for a bucket never versioned.
fn answered(versioning: Versioning) -> Option<mantle_s3::body::Versioning> {
    match versioning {
        Versioning::Unversioned => None,
        Versioning::Enabled => Some(mantle_s3::body::Versioning::Enabled),
        Versioning::Suspended => Some(mantle_s3::body::Versioning::Suspended),
    }
}

/// The reply that states a version's facts, `status` its answer: its metadata among them
/// (research/89 §1).
fn facts(status: Status, order: u64, version: &Version) -> Reply {
    Reply {
        status,
        size: version.size,
        version: name::shown_id(version.null, order),
        etag: version.etag.clone(),
        modified: object::seconds(version.modified_ns),
        checksum: version.checksum.as_ref().map(shown),
        marker: version.marker,
        metadata: version.headers.clone(),
        ..Reply::status(status)
    }
}

/// When, on the shard's clock, the parked requests' askers are next told their requests are held,
/// all at once: at `now` where none was told since none was parked, else a `beat` after they last
/// were at `vouched_ns`, as a held job's asker is (`Job::vouch`'s reasons). A request parked behind
/// others is told within half a beat of parking, as each is half a beat after its last.
fn parked_vouch_ns(vouched_ns: Option<u64>, now: u64, beat: u64) -> u64 {
    vouched_ns.map_or(now, |vouched| vouched.saturating_add(vouch_every(beat)))
}

/// How often a held request's asker is told so, on a `beat`: every half beat. A vouch reaches the
/// asker in the next note on its connection, notes a beat apart, so one told a beat after the last,
/// at a turn just after a note went out, is named two beats after the note before; its asker gives
/// it up a serve period, three beats, after it last heard (hyper-transport `Endpoint::working`,
/// `NOTES_A_PERIOD`). Told every half beat, a held request is named in notes at most a beat and a
/// half apart, the margin it had told at every turn, at two vouches a beat, each a flag.
pub(super) fn vouch_every(beat: u64) -> u64 {
    beat.checked_div(2).unwrap_or(beat).max(1)
}

/// Tells `tell` each of `held`, the askers of work held at once, that it is held, at `now` where
/// half a beat passed since they last were, at `vouched_ns`, or none was told since none was held
/// ([`parked_vouch_ns`]): one call each a half beat, not one each turn. `vouched_ns` becomes `now`
/// where any was told, and none where none is held, so the first held after none is told at its
/// first turn and one held beside others at their next vouch, within half a beat.
pub(super) fn vouch_held<T>(
    vouched_ns: &mut Option<u64>,
    (now, beat): (u64, u64),
    held: impl IntoIterator<Item = T>,
    mut tell: impl FnMut(T),
) {
    if now < parked_vouch_ns(*vouched_ns, now, beat) {
        return;
    }
    let mut told = false;
    for asker in held {
        tell(asker);
        told = true;
    }
    *vouched_ns = if told { Some(now) } else { None };
}

impl<E: Engine + 'static, L: NodeLog> Node<E, L> {
    /// Asks what a job asks (node.md §2.9 step 6b): what it waits on, whose outcome
    /// [`Node::outcome`] gives. Refused `NodeError::Asks` where the node holds as many of its kind
    /// as it keeps (`command_asks`, `read_asks`, `chunk_asks`), which the job asks again at its
    /// next turn.
    pub(super) fn ask(&mut self, ask: JobAsk) -> Result<Pending, NodeError> {
        match ask {
            JobAsk::Command(group, command) => self
                .command(group, command, u64::MAX, Wait::Request)
                .map(Pending::Command),
            JobAsk::Read(group, query) => self
                .read(group, query, u64::MAX, Wait::Request)
                .map(Pending::Read),
            JobAsk::Put {
                at,
                volume,
                key,
                bytes,
                crc32c,
                remaining_ns,
            } => self
                .put_chunk(at, volume, key, bytes, (crc32c, remaining_ns))
                .map(Pending::Chunk),
            JobAsk::Get {
                at,
                volume,
                key,
                span,
                remaining_ns,
                past_suspicion: false,
            } => self
                .get_chunk(at, volume, key, span, remaining_ns)
                .map(Pending::Chunk),
            JobAsk::Get {
                at,
                volume,
                key,
                span,
                remaining_ns,
                past_suspicion: true,
            } => self
                .get_chunk_past_suspicion(at, volume, key, span, remaining_ns)
                .map(Pending::Chunk),
        }
    }

    /// The outcome of what a job asked, once answered, taken.
    fn outcome(&mut self, pending: Pending) -> Option<Outcome> {
        match pending {
            Pending::Command(ask) => take_done(&mut self.commands_done, ask).map(Outcome::Command),
            Pending::Read(ask) => take_done(&mut self.reads_done, ask).map(Outcome::Read),
            Pending::Chunk(ask) => take_done(&mut self.chunks_done, ask).map(Outcome::Chunk),
        }
    }

    /// An object's request arrived on `exchange` from `peer`, its body `body` bytes: a job that
    /// reads its fields first; parked, its body unread, where the host has no room for it now or
    /// requests parked before it wait (`take_parked`), as the S3 listener's door holds a request
    /// (node.md §4.5); or the reply it gets at once.
    pub(super) fn take_object(&mut self, exchange: ExchangeId, peer: PeerId, body: Option<u64>) {
        let head = Head::decode(
            self.driver.endpoint().head(exchange).unwrap_or_default(),
            body,
        );
        let now = hyper_rt::futures::now_ns();
        let deadline_ns = head
            .as_ref()
            .ok()
            .and_then(Head::remaining)
            .map(|remaining| {
                now.saturating_add(u64::try_from(remaining.as_nanos()).unwrap_or(u64::MAX))
            });
        // What it holds at its own size, its body's length known from its head.
        let (charge, window) = match &head {
            Ok(head) => sized(self.gateway.as_deref(), &self.windows, Moves::of_head(head)),
            Err(_) => (0, 0),
        };
        let parks = head.is_ok() && self.gateway.is_some() && !self.stopping();
        // Asked even behind parked requests: one the limit turns away tells it its clients ask
        // more than it admits.
        let room = parks && self.admits(charge);
        if parks && (!room || !self.parked.is_empty()) && self.parked.len() < self.parked_most {
            let patience = u64::try_from(bounds::STOCK_PATIENCE.as_nanos()).unwrap_or(u64::MAX);
            self.parked.push_back(Parked {
                exchange,
                peer,
                head: head.ok(),
                charge,
                window,
                deadline_ns,
                until_ns: deadline_ns.unwrap_or_else(|| now.saturating_add(patience)),
            });
            return;
        }
        // Not parked and no room: a stopping node's, or one whose parked requests are as many as
        // it keeps.
        let slowed = if room {
            None
        } else if self.stopping() {
            Some(door::Slowed::Stopping)
        } else {
            Some(door::Slowed::Queued)
        };
        self.start_object(
            exchange,
            peer,
            head,
            (deadline_ns, now, charge, window),
            slowed,
        );
    }

    /// The parked requests taken in the order they came while the host has room for the first
    /// (`crate::admit`); one whose exchange ended is let go, and one whose client stops waiting
    /// (its deadline, or a stock client's patience) or whose node stops is answered `SlowDown`.
    /// Those still parked are held by the host, as a job whose asks are out is: each tells its
    /// asker so (research/77 D1, D2), all at once half a beat apart ([`parked_vouch_ns`]), at the first
    /// turn they are held since none was.
    pub(super) fn take_parked(&mut self) {
        let now = hyper_rt::futures::now_ns();
        while let Some(&first) = self.parked.front() {
            if self.driver.endpoint().head(first.exchange).is_none() {
                self.parked.pop_front();
                continue;
            }
            let over = now >= first.until_ns || self.stopping();
            if !over && !self.admits(first.charge) {
                let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap_or(u64::MAX);
                if now >= parked_vouch_ns(self.parked_vouched_ns, now, beat) {
                    let Self { parked, driver, .. } = self;
                    for waiting in parked.iter() {
                        driver.endpoint().working(waiting.exchange);
                    }
                    self.parked_vouched_ns = Some(now);
                }
                return;
            }
            self.parked.pop_front();
            let head = first.head.ok_or(ObjectError::Fields(0));
            let slowed = if !over {
                None
            } else if self.stopping() {
                Some(door::Slowed::Stopping)
            } else {
                Some(door::Slowed::Waited)
            };
            self.start_object(
                first.exchange,
                first.peer,
                head,
                (first.deadline_ns, now, first.charge, first.window),
                slowed,
            );
        }
        // None parked: the next to be is told at its first turn held.
        self.parked_vouched_ns = None;
    }

    /// When the node's next turn is due for its parked requests: at once where the host has room
    /// for the first, else at the first's client's patience or the next beat they are vouched at, a
    /// probe timeout, as a held job's (`Job::due_ns`).
    pub(super) fn parked_due(&self, now: u64) -> Option<u64> {
        let first = self.parked.front()?;
        let room = self
            .admission
            .as_ref()
            .is_some_and(|a| a.admits(first.charge).is_ok());
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap_or(u64::MAX);
        Some(if room {
            now
        } else {
            first
                .until_ns
                .min(parked_vouch_ns(self.parked_vouched_ns, now, beat))
        })
    }

    /// The job of an object's request on `exchange` from `peer`, at `now`, its client's deadline
    /// and its charge as its head says: one that reads its fields first where the host admits it
    /// (no `slowed`), else the reply it gets at once, `SlowDown` for `slowed`'s cause where it is
    /// a request the host would run.
    fn start_object(
        &mut self,
        exchange: ExchangeId,
        peer: PeerId,
        head: Result<Head, ObjectError>,
        (deadline_ns, now, charge, window): (Option<u64>, u64, u64, usize),
        slowed: Option<door::Slowed>,
    ) {
        // A client's operations act for the account its certificate names; one whose
        // connections have all gone since asking is no longer known, and nothing is taken.
        let Some(principal) = self.driver.endpoint().directory().principal(peer) else {
            self.driver
                .endpoint()
                .refuse_exchange(exchange, Refusal::Identity);
            return;
        };
        // Its reply goes on a channel the node's task writes from (node.md §2.9 step 6c), a door
        // the host kept from a request before it, or one made (6d): one that cannot be made leaves
        // nothing to reply on, so the exchange is refused, as an owner past what it holds refuses
        // one, which its asker asks again after.
        let Some((mut native, mut replies)) = self.host.doors.take() else {
            self.driver
                .endpoint()
                .refuse_exchange(exchange, Refusal::Budget);
            return;
        };
        // Its job names its exchange on the node's marks as it gives its door anything, so the
        // node's turn visits its reply and feeding alone; the marker kept with the door.
        native.exchange = Some(exchange);
        if native.marker.is_none() {
            native.marker = self.marks.marker();
        }
        let mut admitted_ns = None;
        let mut driven = None;
        let work = match (head, self.gateway.as_deref(), slowed) {
            (_, None, _) => Work::Draining(Box::new(Reply::status(Status::Failed))),
            (Err(_), Some(_), _) => Work::Draining(Box::new(Reply::status(Status::Malformed))),
            // A node stopping begins no operation (node.md §1.7), and one with no room for it
            // answers it at once: its client asks another, or again.
            (Ok(_), Some(_), Some(cause)) => Work::Draining(Box::new(Reply::slowed(cause))),
            // Placed on the shard the rule picks, and counted there until its job ends (node.md
            // §2.9 step 6). A count past a `usize`'s, which no host's jobs reach, each placed
            // request being one of them, places nothing: the request is one the host cannot run.
            // A shard past the first is one a driving task runs on whose shard the watchdog does
            // not see held past its limit (`driving::Lanes::takes`), the lanes in shard order.
            (Ok(head), Some(_), None) => match self.drivers.place(|shard| {
                self.driving
                    .get(shard.wrapping_sub(1))
                    .is_some_and(|lanes| lanes.shard() == shard && lanes.takes())
            }) {
                Some(shard) => {
                    admitted_ns = Some(now);
                    driven = Some(shard);
                    let fields = usize::try_from(head.fields()).unwrap_or(0);
                    Work::Fields(head, Vec::with_capacity(fields))
                }
                None => Work::Draining(Box::new(Reply::status(Status::Failed))),
            },
        };
        let charge = if admitted_ns.is_some() { charge } else { 0 };
        if admitted_ns.is_some() {
            self.charge_admitted(charge);
        }
        // And the node's task names the job to the host as it moves its door, so the host's step
        // passes the job by while it waits on its door alone (`Call`).
        let name = self.host.name_next();
        native.name = Some(name);
        native
            .handed
            .name
            .store(name, std::sync::atomic::Ordering::Release);
        replies.call_by(self.host.caller());
        self.replies.push((exchange, replies));
        self.host.push(
            Job {
                door: Door::Native(Some(exchange), native),
                principal,
                work,
                pending: Vec::new(),
                deadline_ns,
                moved_ns: now,
                vouched_ns: None,
                resting: false,
                admitted_ns,
                charge,
                window,
                driven,
                label: None,
            },
            now,
        );
    }

    /// An operation the S3 listener handed over (`crate::door`): a job of its work, or the reply
    /// it gets at once, as a native request's.
    pub(super) fn take_local(&mut self, request: door::Request) {
        let door::Request {
            op,
            principal,
            denied,
            body,
            reply,
            mark,
        } = request;
        // Its first turn comes as it is pushed: a name before it is let go, so its connection's
        // next names it again.
        if let Some(mark) = &mark {
            mark.taken();
        }
        let now = hyper_rt::futures::now_ns();
        let mut admitted_ns = None;
        let (charge, window) = charged(self.gateway.as_deref(), &self.windows, &op);
        let label = Some(labels::host(&op));
        // The door holds a request while the host has no room (`take_door`): one taken here past
        // the limit is a stopping node's.
        let room = self.gateway.is_some() && !self.stopping() && self.admits(charge);
        let work = match self.gateway.as_deref() {
            None => Work::Draining(Box::new(Reply::status(Status::Failed))),
            // Taken past the limit, the door holds no request while the host has no room: a
            // stopping node's (`take_door`).
            Some(_) if !room => Work::Draining(Box::new(Reply::slowed(door::Slowed::Stopping))),
            Some(_) => {
                admitted_ns = Some(now);
                let block = self.open_block(&op);
                let mut made = work(op, block);
                if let Work::Deletes(deleting) = &mut made {
                    deleting.deny(&denied);
                }
                made
            }
        };
        let charge = if admitted_ns.is_some() { charge } else { 0 };
        if admitted_ns.is_some() {
            self.charge_admitted(charge);
        }
        self.host.push(
            Job {
                door: Door::Local(Box::new(Local {
                    body: Some(Pieces {
                        pieces: body,
                        ended: None,
                        dry: false,
                    }),
                    reply,
                    pending: VecDeque::new(),
                    ready: false,
                    mark,
                })),
                principal,
                work,
                pending: Vec::new(),
                // S3 states no patience: its stock clients' is the bound (research/77 D2).
                deadline_ns: None,
                moved_ns: now,
                vouched_ns: None,
                resting: false,
                admitted_ns,
                charge,
                window,
                // The S3 door's operations are driven where the node's task runs (node.md §2.9).
                driven: None,
                label,
            },
            now,
        );
    }

    /// A key the S3 listener asked for (`door::KeyAsk`): looked up at the next turn, or refused at
    /// once by a node that hosts no objects (`Failed`), a stopping one, or one holding as many
    /// lookups as its door holds requests (`SlowDown`): a connection asks one at a time.
    pub(super) fn take_lookup(&mut self, ask: door::KeyAsk) {
        let door::KeyAsk { id, reply } = ask;
        let refused = if self.gateway.is_none() {
            Some(door::Signed::Refused(Status::Failed))
        } else if self.stopping() {
            Some(door::Signed::Slowed(door::Slowed::Stopping))
        } else if self.lookups.len() >= self.door_capacity {
            Some(door::Signed::Slowed(door::Slowed::Lookups))
        } else {
            None
        };
        if let Some(refused) = refused {
            // A connection gone takes no answer.
            let _ = reply.try_send(refused);
            return;
        }
        self.lookups.push(Lookup {
            id,
            reply,
            ask: None,
        });
    }

    /// The lookups at a turn: each not yet asked read of the root range while the node has room
    /// for a read, and each answered given its answer.
    fn step_lookups(&mut self) {
        let mut at = 0;
        while let Some(lookup) = self.lookups.get(at) {
            let answer = match lookup.ask {
                Some(ask) => take_done(&mut self.reads_done, ask).map(|o| self.signed(o)),
                None => match self.group_of(Layer::Root) {
                    None => Some(door::Signed::Refused(Status::Failed)),
                    Some(group) => {
                        let query = Query::Signer {
                            id: lookup.id.clone(),
                        };
                        match self.read(group, query, u64::MAX, Wait::Request) {
                            Ok(ask) => {
                                if let Some(lookup) = self.lookups.get_mut(at) {
                                    lookup.ask = Some(ask);
                                }
                                None
                            }
                            // Asked again at the next turn a read ends.
                            Err(NodeError::Asks) => None,
                            Err(_) => Some(door::Signed::Refused(Status::Failed)),
                        }
                    }
                },
            };
            match answer {
                Some(answer) => {
                    let lookup = self.lookups.swap_remove(at);
                    let _ = lookup.reply.try_send(answer);
                }
                None => at = at.saturating_add(1),
            }
        }
    }

    /// A lookup's answer from its read: the key's principal, its secret unwrapped under the root
    /// key's generation its row names; none for a key the cell does not hold; refused `SlowDown`
    /// for a root range without a leader that can serve it within its budget (node.md §5.4), and
    /// `Failed` for a row that does not open, which is the range's corruption.
    fn signed(&self, outcome: ReadOutcome) -> door::Signed {
        let failed = door::Signed::Refused(Status::Failed);
        let read = match outcome {
            ReadOutcome::Read(read) => *read,
            ReadOutcome::Deadline => return door::Signed::Slowed(door::Slowed::KeyLeaderless),
            ReadOutcome::Refused(_) => return failed,
        };
        let signer = match read {
            Read::Signer(None) => return door::Signed::Unknown,
            Read::Signer(Some(signer)) => signer,
            _ => return failed,
        };
        let (row, held) = *signer;
        let key = self.gateway.as_deref().and_then(|g| {
            g.keys
                .iter()
                .find(|(generation, _)| *generation == row.generation)
        });
        let Some((_, key)) = key else {
            return failed;
        };
        match mantle_s3::seal::unwrap_secret(&row.wrapped, key) {
            Ok(secret) => door::Signed::Principal(Box::new(door::Signer {
                secret,
                name: held.name,
                id: held.id,
                policy: held.policy,
            })),
            Err(_) => failed,
        }
    }

    /// A job's fields read from its body's start; once whole, the work they name
    /// (`Serve::read_fields`).
    ///
    /// They are read into memory of the job's or the node's task's own, never a piece of the
    /// endpoint's budget, which other traffic may hold past its client's patience (`read_piece`):
    /// into their buffer, made at their length as the job was admitted, while none of them has
    /// come, and else into the node's task's piece (`Node::piece`) as much as is left of them.
    /// Nothing past them is read: a put's body is read by its feeding ([`Feeding`]).
    fn read_fields(&mut self, job: &mut Job) -> Result<bool, NodeError> {
        // A local operation comes typed, never as fields to read, as a native one handed with its
        // fields read does.
        let Door::Native(Some(exchange), _) = &job.door else {
            return Ok(true);
        };
        let exchange = *exchange;
        let Work::Fields(head, fields) = &mut job.work else {
            return Ok(false);
        };
        let wanted = usize::try_from(head.fields()).unwrap_or(usize::MAX);
        let endpoint = self.driver.endpoint();
        while fields.len() < wanted {
            let left = wanted.saturating_sub(fields.len());
            let got = if fields.is_empty() {
                super::read_at_most(endpoint, exchange, fields, left)
            } else {
                let got = super::read_at_most(
                    endpoint,
                    exchange,
                    &mut self.piece,
                    left.min(hyper_transport::RECEIVE_CHUNK),
                );
                fields.extend_from_slice(&self.piece);
                got
            };
            match got {
                Ok(0) if fields.len() < wanted => return Ok(false),
                Ok(read) => {
                    self.moved = self
                        .moved
                        .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
                }
                // Its asker is gone.
                Err(_) => return Ok(true),
            }
        }
        let op = head.op(fields);
        let admitted = job.charge;
        let block = op.as_ref().ok().and_then(|op| self.open_block(op));
        // A copy holds a get's window and a put's at once (`charged`), this job admitted at its
        // head's charge, the most one operation holds: one whose put the operations' memory does
        // not hold beside the rest is answered as one past the host's room is.
        let more = op
            .as_ref()
            .ok()
            .filter(|op| matches!(op, Op::Copy(_) | Op::CopyPart(_)))
            .map_or(0, |op| {
                charged(self.gateway.as_deref(), &self.windows, op)
                    .0
                    .saturating_sub(admitted)
            });
        let crowded = more > 0
            && self
                .admission
                .as_ref()
                .is_some_and(|admission| !admission.holds(more));
        // The bytes past its charge, taken where it runs: the caches give back first what of
        // them they hold.
        let taking = more > 0 && !crowded && op.is_ok() && self.admission.is_some();
        if taking {
            self.charge_more(more);
        }
        // A request placed on a shard a driving task runs on is handed there with its operation
        // (node.md §2.9 step 6e), the shard making its work; its door carries that shard's word
        // and the node's task's waker, each side waking the other as it moves the door (6f).
        // Its job named to that shard's host, and that shard's task woken, as the node's task moves
        // its door (`Caller`).
        let driving = job.driven.and_then(|shard| {
            self.driving
                .iter()
                .find(|lanes| lanes.shard() == shard)
                .map(super::driving::Lanes::caller)
        });
        job.work = match op {
            Err(_) => Work::Draining(Box::new(Reply::status(Status::Malformed))),
            Ok(_) if crowded => Work::Draining(Box::new(Reply::slowed(door::Slowed::Crowded))),
            Ok(op) => {
                if taking {
                    job.charge = job.charge.saturating_add(more);
                }
                job.label = Some(labels::host(&op));
                if driving.is_some() {
                    Work::Handing(Box::new((op, block)))
                } else {
                    work(op, block)
                }
            }
        };
        // The put's body comes on its door's channel, its sender lent to the feeding: one whose door
        // lent it already, which no put's does, is answered as one whose channel cannot be made.
        let (body, handing) = match &job.work {
            Work::Put(putting) => (Some((putting.length, putting.trailing)), false),
            Work::Handing(handing) => (body_of(&handing.0), true),
            _ => (None, false),
        };
        let Door::Native(_, native) = &mut job.door else {
            return Ok(false);
        };
        let lends = body.is_some();
        let held = self.replies.change(exchange, |replies| {
            if handing && driving.is_some() {
                replies.call_by(driving);
            }
            if !lends {
                return None;
            }
            Some((replies.body.take()?, replies.call.clone()))
        });
        if handing && held.is_some() {
            native.node = Some(self.waker.clone());
        }
        if let Some(body) = body {
            match held.flatten() {
                Some((pieces, call)) => {
                    let handed = std::sync::Arc::clone(&native.handed);
                    self.feeding
                        .push((exchange, feed(body, pieces, handed, call)));
                }
                None => job.work = Work::Draining(Box::new(Reply::slowed(door::Slowed::Door))),
            }
        }
        Ok(false)
    }

    /// For a native upload's Open, the plaintext bytes of a whole block of the layout new blocks
    /// take, the unit its runs are cut in (`name::run_length`); none for any other operation, or
    /// where the volume table names no scheme yet.
    fn open_block(&mut self, op: &Op) -> Option<u64> {
        if !matches!(op, Op::Open { .. }) {
            return None;
        }
        let scheme = self.table().ok()?.scheme().ok()??;
        let segments = Layout::new(scheme).ok()?.segments_per_block();
        segments.checked_mul(u64::try_from(mantle_s3::seal::SEGMENT).ok()?)
    }

    /// The group of the range this node hosts of `layer`: the cell's one range of it.
    pub(super) fn group_of(&self, layer: Layer) -> Option<u128> {
        self.ranges
            .iter()
            .find(|k| k.layer == layer)
            .map(|k| k.group)
    }

    /// The volume table as this node's member of the root range holds it now: read from the
    /// range's rows again only once the member applied an entry since the last read. Where another
    /// shard drives the root range, the table its shard last answered, asked again once the range
    /// shows it applied past it ([`shard::ask_table`]), the one held read meanwhile: the node read
    /// one before the range moved (`Node::spread_to`). At each look the scheme new blocks take is
    /// settled as far as it can be without waiting (`Scheming::settle`): an answer the compute pool
    /// gave taken, and a choice asked where the domains changed.
    pub(super) fn table(&mut self) -> Result<&Table, NodeError> {
        let (handle, group) = self
            .ranges
            .iter()
            .find(|k| k.layer == Layer::Root)
            .map(|k| (k.handle, k.group))
            .ok_or(NodeError::Bounds("no root range here".into()))?;
        if let Some(far) = self.far.iter_mut().find(|far| far.group == group) {
            if let Some(answered) = far.table.take() {
                let shard::Facts { applied, facts } = *answered?;
                self.table = Some(Table::of(self.table.take(), applied, facts));
            }
            let held = self.table.as_ref().map(|t| t.applied);
            match shard::ask_table(&self.shards, far, held) {
                Ok(()) | Err(NodeError::LaneFull(_)) => {}
                Err(e) => return Err(e),
            }
        } else {
            let shell = self
                .owner
                .get_mut(handle)
                .ok_or(NodeError::Bounds("no root range here".into()))?;
            let applied = shell.applied().index;
            if self.table.as_ref().is_none_or(|t| t.applied != applied) {
                let facts = shard::read_facts(shell.machine_mut().engine_mut())?;
                self.table = Some(Table::of(self.table.take(), applied, facts));
            }
        }
        let table = self
            .table
            .as_mut()
            .ok_or(NodeError::Bounds("no volume table".into()))?;
        table
            .scheming
            .settle(self.gateway.as_deref(), &table.standing);
        Ok(table)
    }

    /// The host at a turn, lent out for its step ([`Host::step`]) after the answers to asks whose
    /// jobs ended are dropped and the S3 door's key lookups stepped.
    pub(super) fn step_host(&mut self) -> Result<(), NodeError> {
        self.drop_abandoned();
        self.step_lookups();
        // A chunk answered otherwise than for want of room frees room at its volume and in the
        // transport toward it, as a volume's put or get answered does at the volume: read once a
        // turn, for the host's jobs and the driving shards' (`serve_driving`).
        self.room_freed |= self
            .chunks_done
            .iter()
            .any(|(_, outcome)| taken(outcome) != Taken::Room);
        self.lend(|host, node| host.step(node))
    }

    /// Each native request whose fields were read for a driving shard handed there (node.md §2.9
    /// step 6e), with its operation, its door's job half and its admission; one its lane refuses
    /// for room, or whose shard holds no lane, made into work here and run on the first shard.
    /// The node's task keeps its exchange: its body's feeding and its reply's half.
    pub(super) fn hand_off(&mut self) {
        // A request is made `Handing` only where a lane is held for its shard.
        if self.driving.is_empty() {
            return;
        }
        // Each handover taken once: one its lane refuses runs here as a job pushed last, its work
        // made from its operation, not handed again.
        while let Some((request, exchange)) = self.host.handing() {
            let lanes = self
                .driving
                .iter()
                .find(|lanes| Some(lanes.shard()) == request.driven);
            let refused = match lanes {
                Some(lanes) => lanes.hand(request),
                None => Some(request),
            };
            // Run here: counted on the first shard, which runs it, until its job ends; its door
            // wakes no shard, as the node's task steps its job in its turn.
            if let Some(mut request) = refused {
                if let Some(from) = request.driven
                    && self.drivers.moved(from, 0).is_some()
                {
                    request.driven = Some(0);
                }
                request.half.node = None;
                if let Some(exchange) = exchange {
                    // Named to the node's own host from here, as it was before it was handed.
                    let call = self.host.caller();
                    self.replies
                        .change(exchange, |replies| replies.call_by(call.clone()));
                    if let Some((_, feeding)) = self.feeding.get_mut(exchange) {
                        feeding.call = call;
                    }
                }
                self.host.take_handed(request, exchange);
            }
        }
    }

    /// Each driving shard's lanes at the node's turn (node.md §2.9 step 6e), served by the node as
    /// its own host is: each ask asked, held while the node has no room, each answer sent, each
    /// job's end told and its door's half kept in the host's doors, and the volume table, where the
    /// node read a newer one, told. A shard whose task ended stops the node.
    pub(super) fn serve_driving(&mut self) -> Result<(), NodeError> {
        // Room freed this turn is offered once: to the host's jobs at its step, to a driving
        // shard's at this serve, and only to a shard a chunk of whose jobs waits for room
        // (`Lanes::tell_freed`): told to every shard, each chunk answered woke every driving shard
        // to move nothing, the S3 door's requests and other shards' among them.
        let freed = std::mem::take(&mut self.room_freed);
        if self.driving.is_empty() {
            return Ok(());
        }
        // Read again where its root range applied past it, as the node's own host reads it at
        // each put it places: a driving shard's puts are placed by the table the node last read,
        // which the node's own host may not read while the driving shards run its puts. One the
        // node cannot read leaves the shards the last told.
        let applied = self.table().ok().map(Table::applied);
        let told = applied.filter(|applied| Some(*applied) != self.driving_table);
        let mut driving = std::mem::take(&mut self.driving);
        let mut host = std::mem::take(&mut self.host);
        let mut served = Ok(());
        for lanes in &mut driving {
            if told.is_some()
                && let Some(table) = self.table.as_ref()
            {
                lanes.tell_table(Box::new(table.settled()));
            }
            if freed {
                lanes.tell_freed();
            }
            served = lanes.serve(self, |half| host.doors.give_native(half));
            if served.is_err() {
                break;
            }
        }
        if told.is_some() {
            self.driving_table = told;
        }
        self.host = host;
        self.driving = driving;
        served
    }

    /// The host lent out to `step`, served by the node through [`Serve`], and taken back
    /// (node.md §2.9 step 6d): `std::mem::take` leaves the default host in its place, which holds
    /// nothing and allocates nothing, and the host is put back whatever the step came to.
    fn lend<T>(&mut self, step: impl FnOnce(&mut Host, &mut Self) -> T) -> T {
        let mut host = std::mem::take(&mut self.host);
        let stepped = step(&mut host, self);
        self.host = host;
        stepped
    }

    /// The node's task's half of each native reply (node.md §2.9 step 6c): what its job gave
    /// written to its exchange as far as the exchange takes it, after the host's step and the
    /// bodies' feeding at each turn ([`Replies`]), of the replies marked since the last
    /// ([`Writes`]): each at its push, as its job gives it anything or ends (`Native::mark`), as
    /// its exchange takes more or its request's body arrives (`Node::on_event`), as its feeding
    /// ends, at each turn while its rounds ran out, and, while it waits for room in the endpoint's
    /// budget, as the budget gives bytes back or its next vouch comes. A walk of every reply
    /// visited thousands that had nothing to write. A get's job gives a piece only once the last
    /// is written whole, so where one is and the exchange takes more, its job gives the next
    /// within the turn: as many of its pieces go in a turn as the exchange takes, as when its job
    /// wrote them, at most a window's (`GatewaySettings::window`, the blocks its driver holds read
    /// at once) and the end past them. One whose rounds ran out makes the next turn due at once.
    /// One over, ended, refused or let go, is let go.
    pub(super) fn write_replies(&mut self) -> Result<(), NodeError> {
        self.replied = false;
        self.take_marks();
        let read = self.wake_rooms();
        if self.replies.begin_visits() == 0 {
            return Ok(());
        }
        // The shard's clock, read once for the turn's replies, as the host's step reads it; the
        // wall clock as well, once a job's step wants it.
        let now = read.unwrap_or_else(clock);
        let mut wall = None;
        let rounds = self
            .gateway
            .as_ref()
            .map_or(0, |gateway| gateway.window)
            .saturating_add(2);
        while let Some((exchange, at)) = self.replies.next_visit() {
            let mut left = rounds;
            // Whether it is visited at the next turn with nothing more said: its rounds ran out.
            // One waiting for room in the endpoint's budget is visited as the budget gives bytes
            // back or at its next vouch (`Node::wake_rooms`).
            let mut again = false;
            let over = loop {
                match self.write_reply(at, now) {
                    Wrote::Over => break true,
                    Wrote::Still | Wrote::Draining | Wrote::Room => break false,
                    Wrote::More => {
                        left = left.saturating_sub(1);
                        if left == 0 {
                            self.replied = true;
                            again = true;
                            break false;
                        }
                        // A job that ended gave all it will: the next write finds its channel
                        // ended.
                        if let Some(job) = self.host.job_of(exchange) {
                            let wall = *wall.get_or_insert_with(|| self.wall.now_ns());
                            self.lend(|host, node| {
                                host.turn_job(node, job, now, wall, &mut Timing::from(now))
                            })?;
                        }
                    }
                }
            };
            if over {
                let Some((exchange, mut replies)) = self.replies.take(at) else {
                    continue;
                };
                if let Some((_, feeding)) = self.feeding.remove(exchange) {
                    replies.body = Some(feeding.let_go());
                }
                self.host.doors.give_replies(replies);
            } else if again {
                self.replies.mark(exchange);
            }
        }
        Ok(())
    }

    /// Marks the replies waiting for room in the endpoint's budget that this turn moves
    /// (`Writes::wake_rooms`): every one where the budget gave bytes back since a visit last found
    /// it short, which made this turn due at once (`Node::host_due`), else each whose next vouch
    /// or client's patience came. The shard's clock read where one waits, for the turn's replies.
    fn wake_rooms(&mut self) -> Option<u64> {
        self.replies.first()?;
        let used = self.driver.endpoint().budget().used();
        let now = clock();
        self.replies.wake_rooms(used, now);
        Some(now)
    }

    /// The exchanges the node's marks name since the last turn's, each one's reply and feeding
    /// marked to visit, then its door's word let go, so what its job gives after names it again
    /// (`Marks`); every reply and feeding where a mark found no room. At most the marks' room a
    /// call: a job naming its exchange again as fast as it is taken waits for the next turn, which
    /// its waker makes due.
    fn take_marks(&mut self) {
        if self.marks.missed() {
            self.feeding.mark_all();
            self.replies.mark_all();
        }
        for _ in 0..self.marks.room() {
            let Some(exchange) = self.marks.take() else {
                break;
            };
            self.feeding.mark(exchange);
            self.replies.marked_by_job(exchange);
        }
    }

    /// One native reply written as far as its exchange takes it at the turn of `now`: a reply with
    /// no bytes past its fields once the request's body is read whole and dropped, after its
    /// feeding, as `drain_job` wrote one; what else its job gave as it comes.
    fn write_reply(&mut self, at: usize, now: u64) -> Wrote {
        let Some(exchange) = self.replies.at(at).map(|(exchange, _)| *exchange) else {
            return Wrote::Over;
        };
        let mut body_read = false;
        // A reply taken, then its body read: the second pass writes it.
        for _ in 0..2 {
            let mut room = false;
            let draining = self
                .replies
                .at(at)
                .filter(|(_, replies)| replies.draining())
                .map(|(_, replies)| replies.admitted());
            if let Some(admitted) = draining {
                // A body the node's task reads for the job is read and dropped there.
                if self.feeding.contains(exchange) {
                    return Wrote::Still;
                }
                match self.drop_body(exchange, admitted) {
                    // It drained what came and waits for the rest of its body, room or none.
                    Fed::Still => {
                        self.replies.change_at(at, |replies| replies.room = None);
                        return Wrote::Still;
                    }
                    // Refused at the bytes the budget holds now: it fits only once fewer are.
                    Fed::Room => {
                        let used = self.driver.endpoint().budget().used();
                        self.replies.short_of(used);
                        room = true;
                    }
                    // Its asker is gone: nothing to answer.
                    Fed::Moved | Fed::Over if !self.driver.endpoint().body_complete(exchange) => {
                        return Wrote::Over;
                    }
                    Fed::Moved | Fed::Over => body_read = true,
                }
            }
            let Self {
                replies,
                driver,
                moved,
                slowed,
                windows,
                ..
            } = self;
            let mut toward = Toward {
                endpoint: driver.endpoint(),
                exchange,
            };
            let wrote = replies.change_at(at, |replies| {
                if room {
                    return replies.wait_for_room(&mut toward, now, room_beat());
                }
                let wrote = replies.write(&mut toward, body_read, now, moved, slowed);
                // What a get's client took of a piece once its room was full: its pace, for the
                // windows the node's next gets hold (`crate::window`).
                if let Some((bytes, ns)) = replies.take_paced() {
                    windows.get.paced(bytes, ns);
                }
                wrote
            });
            match wrote {
                None => return Wrote::Over,
                Some(wrote) if room => return wrote,
                Some(Wrote::Draining) => {}
                Some(wrote) => return wrote,
            }
        }
        Wrote::Draining
    }

    /// The exchange's half of each native put's body (node.md §2.9 step 6c): read from its exchange
    /// and given to its job as far as the job takes it, after the host's step in which the job took
    /// what it was given ([`Feeding`]), of the feedings marked since the last ([`Feedings`]):
    /// each at its push, as its job takes a piece or lets go of its body (`Native::mark`), and as
    /// bytes arrive on its exchange (`Node::on_event`). A walk of every feeding visited thousands
    /// whose job still held its last piece. One that moved makes the node's next turn due at once
    /// (`host_due`), so its job takes what it was given; one ended, given or dropped whole or its
    /// exchange failed, is let go, its reply marked.
    pub(super) fn feed_bodies(&mut self) {
        self.fed = false;
        self.take_marks();
        self.feeding.begin_visits();
        while let Some(at) = self.feeding.next_visit() {
            match self.feed_body(at) {
                Fed::Still | Fed::Room => {}
                Fed::Moved => self.fed = true,
                Fed::Over => {
                    self.fed = true;
                    let Some((exchange, feeding)) = self.feeding.take(at) else {
                        continue;
                    };
                    let sender = feeding.let_go();
                    // Its reply, which waited for the body read whole, is written next.
                    self.replies
                        .change(exchange, |replies| replies.body = Some(sender));
                    self.replies.mark(exchange);
                }
            }
        }
    }

    /// One native put's body read from its exchange as far as its job takes it: a piece at a time
    /// while the job let go of the last, the object's bytes given and the trailer's value kept,
    /// then the body's end once the transport verified it whole, its trailer's checksum with it.
    /// A job that let go of its body has the rest read and dropped, so the exchange ends whole for
    /// its reply, as the job drained it when it read its exchange; an exchange that fails tells the
    /// job its body did not come whole (`IncompleteBody`), as the job heard it then.
    fn feed_body(&mut self, at: usize) -> Fed {
        let Self {
            feeding,
            driver,
            moved,
            piece,
            ..
        } = self;
        let Some((exchange, feeding)) = feeding.at_mut(at) else {
            return Fed::Over;
        };
        #[cfg(test)]
        feeding
            .handed
            .took
            .store(0, std::sync::atomic::Ordering::Release);
        let exchange = *exchange;
        let endpoint = driver.endpoint();
        let mut fed = Fed::Still;
        loop {
            match feeding.send_held() {
                None => {}
                Some(Fed::Still | Fed::Room) => return fed,
                Some(Fed::Over) => return Fed::Over,
                Some(Fed::Moved) => fed = Fed::Moved,
            }
            if feeding.job_let_go() {
                feeding.held = None;
                feeding.dropping = true;
            }
            if feeding.dropping {
                break;
            }
            if feeding.read_whole() {
                if !feeding.empty && !endpoint.body_complete(exchange) {
                    // The body's checksum, after its last byte.
                    let mut empty = hyper_transport::Reservation::default();
                    let _ = endpoint.read_body(exchange, &mut empty);
                    if !endpoint.body_complete(exchange) {
                        return fed;
                    }
                }
                match feeding.end() {
                    Fed::Still => return fed,
                    _ => return Fed::Over,
                }
            }
            if !feeding.free() {
                return fed;
            }
            // Into the node's task's piece, then the feeding's storage: read whenever the job let
            // go of the last piece and its client has sent more, whatever other traffic holds of
            // the endpoint's budget.
            let got = super::read_piece(endpoint, exchange, piece);
            let given = match got {
                Ok(0) => Ok(Fed::Still),
                Ok(_) => feeding.give(piece, moved),
                Err(_) => Err(Status::IncompleteBody),
            };
            match (got, given) {
                (Ok(0), _) => return fed,
                (_, Ok(Fed::Still)) => {}
                (_, Ok(_)) => fed = Fed::Moved,
                // The exchange failed: its job hears its body did not come whole, and ends with
                // the exchange.
                (Err(_), Err(status)) => {
                    let _ = feeding.pieces.try_send(Piece::Refused(status));
                    return Fed::Over;
                }
                // Bytes past the trailer's value, which the transport's length of the body rules
                // out: refused as the job refused them, the rest dropped for its reply.
                (Ok(_), Err(status)) => {
                    feeding.send(Piece::Refused(status));
                    feeding.dropping = true;
                    fed = Fed::Moved;
                }
            }
        }
        match self.drop_body(exchange, true) {
            Fed::Still | Fed::Room => fed,
            dropped => dropped,
        }
    }

    /// What is left of a body its job let go of, read and dropped as it arrives, so its exchange
    /// ends whole for the job's reply (`drain_job`): `Over` once the transport verified it whole,
    /// or the exchange is gone. A body read whole, as a put's that ended is, needs no piece. One
    /// its job was admitted for (`own`) is read into the node's task's piece (`Node::piece`); one
    /// answered at once holds no charge, and reads through the endpoint's one piece for its
    /// owner's reads (`bounds::messages`), which waits for room while other reads hold it (`Room`).
    fn drop_body(&mut self, exchange: ExchangeId, own: bool) -> Fed {
        let endpoint = self.driver.endpoint();
        while !endpoint.body_complete(exchange) {
            let got = if own {
                super::read_piece(endpoint, exchange, &mut self.piece)
            } else {
                let Ok(mut piece) = endpoint.reserve(Class::Transfer, PIECE) else {
                    return Fed::Room;
                };
                let got = endpoint.read_body(exchange, &mut piece);
                endpoint.release(piece);
                got
            };
            match got {
                Ok(0) => break,
                Ok(read) => {
                    self.moved = self
                        .moved
                        .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
                }
                Err(_) => return Fed::Over,
            }
        }
        if !endpoint.body_complete(exchange) && endpoint.head(exchange).is_some() {
            let mut empty = hyper_transport::Reservation::default();
            let _ = endpoint.read_body(exchange, &mut empty);
        }
        if endpoint.body_complete(exchange) || endpoint.head(exchange).is_none() {
            Fed::Over
        } else {
            Fed::Still
        }
    }

    /// The outcomes of asks whose job ended before they came, dropped as they come: an outcome no
    /// job takes would hold one of the node's asks for good.
    fn drop_abandoned(&mut self) {
        if self.abandoned.is_empty() {
            return;
        }
        // Each answer waiting probes the asks let go, rather than each ask let go searching the
        // answers.
        let Self {
            abandoned,
            commands_done,
            reads_done,
            chunks_done,
            ..
        } = self;
        commands_done.retain(|(ask, _)| !abandoned.remove(&Pending::Command(*ask)));
        reads_done.retain(|(ask, _)| !abandoned.remove(&Pending::Read(*ask)));
        chunks_done.retain(|(ask, _)| !abandoned.remove(&Pending::Chunk(*ask)));
    }

    /// Whether the host holds `exchange`, for an event on it: a job's, or a reply its node's task
    /// writes.
    pub(super) fn hosts(&self, exchange: ExchangeId) -> bool {
        self.host.job_of(exchange).is_some() || self.replies.contains(exchange)
    }

    /// An exchange of a job ended unanswered: the job ends, what it waits on let go.
    pub(super) fn job_lost(&mut self, exchange: ExchangeId) {
        let Self {
            host,
            abandoned,
            admission,
            drivers,
            parked,
            feeding,
            replies,
            ..
        } = self;
        let now = hyper_rt::futures::now_ns();
        parked.remove(exchange);
        // An exchange's one feeding, its body's sender given back with its door's half.
        let mut lent = feeding
            .remove(exchange)
            .map(|(_, feeding)| feeding.let_go());
        // Its door's halves given back, kept for the next request, its body's sender with it.
        if let Some((_, mut lost)) = replies.remove(exchange) {
            if let Some(sender) = lent.take() {
                lost.body = Some(sender);
            }
            host.doors.give_replies(lost);
        }
        // An exchange's one job, its request's.
        if let Some(j) = host.job_of(exchange).and_then(|at| host.remove(at)) {
            abandoned.extend(j.pending.into_iter().map(|(pending, _)| pending));
            if let Some(shard) = j.driven {
                drivers.ended(shard);
            }
            if let (Some(at), Some(admission)) = (j.admitted_ns, admission.as_mut()) {
                admission.completed(now.saturating_sub(at), now, j.charge);
            }
            if let Door::Native(_, native) = j.door {
                host.doors.give_native(native);
            }
        }
    }

    /// The operations the host admits at once now, as it learned its knee (`crate::admit`): none
    /// for a node that hosts no objects.
    pub fn admitting(&self) -> Option<usize> {
        self.admission.as_ref().map(crate::admit::Admission::limit)
    }

    /// Whether the host admits one more operation holding `charge` bytes beside those it runs
    /// (`crate::admit`). One the learned limit turns away tells the limit its clients ask more
    /// than it admits; one whose bytes the operations' memory does not hold says nothing of it.
    pub(super) fn admits(&mut self, charge: u64) -> bool {
        let Some(admission) = self.admission.as_mut() else {
            return true;
        };
        match admission.admits(charge) {
            Ok(()) => true,
            Err(crate::admit::Held::Limit) => {
                admission.turned_away();
                false
            }
            Err(crate::admit::Held::Bytes) => false,
        }
    }

    /// The ranges' page caches on the node's shard lent their parts of the operations' idle
    /// bytes once `more` are charged ([`lend`]), growing where `grow`; none for a node that hosts
    /// no objects, which has no operations' memory to lend.
    pub(super) fn lend_caches(&mut self, more: u64, grow: bool) {
        let Self {
            gateway,
            admission,
            ranges,
            owner,
            ..
        } = self;
        if let (Some(gateway), Some(admission)) = (gateway.as_deref(), admission.as_mut()) {
            lend(gateway.cache_bytes, admission, ranges, owner, more, grow);
        }
    }

    /// An operation admitted, charged `charge` bytes: the caches give back first what of them
    /// they hold.
    fn charge_admitted(&mut self, charge: u64) {
        self.lend_caches(charge, false);
        if let Some(admission) = self.admission.as_mut() {
            admission.admitted(charge);
        }
    }

    /// A running operation takes `more` bytes past its charge, or the cell's repair takes them
    /// for a rebuild: the caches give back first what of them they hold.
    pub(super) fn charge_more(&mut self, more: u64) {
        self.lend_caches(more, false);
        if let Some(admission) = self.admission.as_mut() {
            admission.took(more);
        }
    }

    /// An operation the host admitted at `admitted_ns`, charged `charge` bytes, ended: the limit
    /// learns its latency, and its bytes are free.
    fn ended(&mut self, admitted_ns: Option<u64>, charge: u64) {
        let now = hyper_rt::futures::now_ns();
        if let (Some(at), Some(admission)) = (admitted_ns, self.admission.as_mut()) {
            admission.completed(now.saturating_sub(at), now, charge);
        }
        self.keep_pool();
    }

    /// The native clients' stream pool sized to the limit the admission learned, where it moved:
    /// the exchanges they may open past those they keep are the operations the host runs
    /// (Breakwater's credit, hyper-transport's `pool`).
    pub(super) fn keep_pool(&mut self) {
        let Some(limit) = self.admission.as_ref().map(crate::admit::Admission::limit) else {
            return;
        };
        if self.pooled == 0 || self.pooled == limit {
            return;
        }
        let streams = u32::try_from(limit).unwrap_or(u32::MAX);
        if self.driver.endpoint().set_stream_pool(streams).is_ok() {
            self.pooled = limit;
        }
    }

    /// When the node's task next needs a turn for its host and the exchanges it keeps for it: its
    /// host's own wait ([`Host::due`]: its jobs' answered asks, its puts' renewals and its held
    /// jobs' beats and bounds), beside what the node's task waits on itself (node.md §2.9 step
    /// 6e): at once where an ask of a job that ended is answered, its outcome to drop, a lookup is
    /// answered, a body was fed at the last turn (its job takes what it was given), or a reply's
    /// exchange took more than its rounds gave (its job gives the next); and a reply that waits for
    /// the piece its request's body drains through, room in the endpoint's budget: at once where
    /// the budget gave bytes back since a visit found it short (`Writes::room_freed`), as the
    /// turn's own work gives them back (a piece read and released, an exchange ended), else at its
    /// next vouch, half a beat after its last, or its client's patience. The budget is the node's
    /// task's own: what its turn gives back is read as the turn ends, and what its transport gives
    /// back between turns, the driver telling no event of it, at the next turn, at the latest that
    /// next vouch.
    pub(super) fn host_due(&mut self) -> Option<u64> {
        let shard = hyper_rt::futures::now_ns();
        // A reply waiting for room whose budget gave bytes back this turn, after its replies were
        // written: one probe of the budget's count.
        let used = self.driver.endpoint().budget().used();
        if self.replies.room_freed(used) {
            return Some(shard);
        }
        // A probe of the reads answered for each lookup asked, at most the door's capacity, which
        // the turn's `step_lookups` walks as well.
        let looked_up = self
            .lookups
            .iter()
            .filter_map(|l| l.ask)
            .any(|ask| self.reads_done.contains(ask));
        // Each answer waiting probes the asks let go, as `drop_abandoned` drops them.
        let dropped = !self.abandoned.is_empty()
            && self
                .answers()
                .any(|pending| self.abandoned.contains(&pending));
        if self.fed || self.replied || looked_up || dropped {
            return Some(shard);
        }
        // The earliest reply waiting for room, its heap's first (`Writes`).
        [self.host.due(self), self.replies.first()]
            .into_iter()
            .flatten()
            .min()
    }
}

/// The node's task serves the host it lends out (node.md §2.9 step 6d): each of the host's asks
/// by the node's own functions, as the host's steps called them when they were the node's.
impl<E: Engine + 'static, L: NodeLog> Serve for Node<E, L> {
    fn ask(&mut self, ask: JobAsk) -> Result<Pending, NodeError> {
        Node::ask(self, ask)
    }

    fn outcome(&mut self, pending: Pending) -> Option<Outcome> {
        Node::outcome(self, pending)
    }

    fn answers(&self) -> impl Iterator<Item = Pending> {
        let commands = self
            .commands_done
            .iter()
            .map(|(ask, _)| Pending::Command(*ask));
        let reads = self.reads_done.iter().map(|(ask, _)| Pending::Read(*ask));
        let chunks = self.chunks_done.iter().map(|(ask, _)| Pending::Chunk(*ask));
        commands.chain(reads).chain(chunks)
    }

    fn abandon(&mut self, asks: impl IntoIterator<Item = Pending>) {
        self.abandoned.extend(asks);
    }

    fn table(&mut self) -> Result<&Table, NodeError> {
        Node::table(self)
    }

    fn group_of(&self, layer: Layer) -> Option<u128> {
        Node::group_of(self, layer)
    }

    fn ranges(&self) -> usize {
        self.ranges.len()
    }

    fn command_room(&self) -> bool {
        Node::command_room(self)
    }

    fn room(&self, ask: &JobAsk) -> bool {
        match ask {
            JobAsk::Command(..) => Node::command_room(self),
            JobAsk::Read(..) => {
                self.reads_out.len().saturating_add(self.reads_done.len()) < self.read_asks
            }
            JobAsk::Put { at, .. } | JobAsk::Get { at, .. } => {
                *at == self.id || self.chunks_out.len() < self.chunk_asks
            }
        }
    }

    fn command_asks(&self) -> usize {
        self.command_asks
    }

    fn wall_ns(&self) -> u64 {
        self.wall.now_ns()
    }

    /// Room freed since the last turn, as the turn's step read it (`Node::step_host`): each chunk
    /// refused for want of room is asked again then (research/77 D2), and not at its own refusal,
    /// which would ask it again each turn while the room stays taken. A driving shard's puts and
    /// gets wait on the same room, told it at the same turn (`Node::serve_driving`).
    fn freed(&mut self) -> bool {
        self.room_freed
    }

    fn moved(&self) -> u64 {
        self.moved
    }

    fn read_fields(&mut self, job: &mut Job) -> Result<bool, NodeError> {
        Node::read_fields(self, job)
    }

    fn took(&mut self, paced: door::Paced) {
        if let Some(doorway) = self.doorway.as_mut() {
            doorway.took(paced);
        }
    }

    fn committed(&mut self, ns: u64) {
        self.windows.put.took(ns);
    }

    fn filled(&mut self, bytes: u64, ns: u64) {
        self.windows.put.paced(bytes, ns);
    }

    fn read(&mut self, ns: u64) {
        self.windows.get.took(ns);
    }

    fn windows(&self) -> crate::window::Windows {
        self.windows
    }

    fn holds(&self, more: u64) -> bool {
        self.admission
            .as_ref()
            .is_none_or(|admission| admission.holds(more))
    }

    fn recharge(&mut self, more: u64, less: u64) {
        self.charge_more(more);
        if less > 0
            && let Some(admission) = self.admission.as_mut()
        {
            admission.gave(less);
        }
    }

    fn job_ended(
        &mut self,
        admitted_ns: Option<u64>,
        charge: u64,
        driven: Option<usize>,
        asks: impl IntoIterator<Item = Pending>,
        half: Option<Box<Native>>,
    ) -> Option<Box<Native>> {
        self.ended(admitted_ns, charge);
        if let Some(shard) = driven {
            self.drivers.ended(shard);
        }
        self.abandoned.extend(asks);
        half
    }
}

impl Host {
    /// The host at a turn: each operation's answers taken, its next requests asked, its body read
    /// and its bytes written as far as they go, and each one done answered, served by `node`.
    ///
    /// The task is labelled, as the turn ends, with the operation whose step held it longest, none
    /// where it stepped none (`crate::labels`): a long poll of the task is counted under the
    /// operation that cost it most. Only a step that did work is timed ([`Timing`]).
    pub(super) fn step<N: Serve>(&mut self, node: &mut N) -> Result<(), NodeError> {
        // The jobs whose doors the node's task moved, woken before any is stepped, so a step sees
        // each move named before it (`Host::take_calls`).
        self.take_calls();
        if self.jobs.is_empty() {
            labels::set(None);
            return Ok(());
        }
        // The shard's clock, read once for the turn: every answer, bound, offer of room and moved
        // job of the turn is stamped with it, a turn lasting microseconds against bounds of a
        // beat or a client's patience.
        let now = clock();
        // Every operation's answers taken before any asks again, so each asks with the room
        // that every answer taken this turn gave back: the jobs answered found by their asks'
        // slots (`asks`), each once, where a walk of every job looked up each one's asks.
        let freed = node.freed();
        let mut answered = std::mem::take(&mut self.answered);
        answered.clear();
        answered.extend(node.answers().filter_map(|pending| self.asks.get(&pending)));
        answered.sort_unstable();
        answered.dedup();
        for &at in &answered {
            self.take_answers(node, at, now)?;
            // Keyed from what its answers made of it, a chunk refused for room among the waiters
            // before room is offered.
            self.rekey(at, now);
        }
        self.answered = answered;
        // A chunk refused for room at a peer's volume hears nothing when that volume frees room
        // (its queue held by others' writes or by its cleaner, as a filling volume's is), and with
        // no other chunk of this node answered nothing offered it room again before its client's
        // patience: it is offered room again at each beat it waits, a probe timeout, as a held
        // job is vouched for (research/77 D2), at most once a beat. A peer holds a put it has no
        // room for on its exchange and reads it as room frees (`node::hold`): the beat serves the
        // puts refused past what it holds, and a volume's `Busy`.
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap_or(u64::MAX);
        let waiting = !self.rooms.is_empty();
        let due = waiting && now >= self.room_offered_ns.saturating_add(beat);
        if freed || due {
            self.offer_room(node, now)?;
        }
        if freed || due || !waiting {
            self.room_offered_ns = now;
        }
        self.bound_jobs(node, now, beat)?;
        // The wall clock, read once for the turn as the shard's is: a put's renewals and the
        // times its records carry are the turn's, read once where each step read it, every job
        // stepped every turn, each read through `SystemTime::now` (macOS `sample` of the 1-byte
        // PUT bench's three-node cell, 2026-11-19: 14% of the first shard, 98% of it under
        // `step_job`).
        let wall = node.wall_ns();
        // A renewal is the one event of a resting put's that no answer or bound brings: each
        // one due wakes its job, the renewals due alone read from those kept (`renewals`).
        let mut due = std::mem::take(&mut self.due);
        self.renewals.due(wall, &mut due);
        for &at in &due {
            self.wake(at);
        }
        self.due = due;
        // The jobs awake as the steps begin, each stepped once, the first woken first: one that
        // does not rest after its step is woken again after them, for the next turn, and one
        // that ends leaves its slot to the job moved into it, whose entry names that slot. A job
        // that rests is not read: a turn's steps cost by the jobs woken, not the jobs held.
        let mut timing = Timing::from(now);
        let woken = self.woken;
        for _ in 0..self.jobs.len() {
            let Some((key, at)) = self.ready.least() else {
                break;
            };
            if key >= woken {
                break;
            }
            #[cfg(test)]
            tests::READS.with(|reads| reads.set(reads.get().saturating_add(1)));
            self.turn_job(node, at, now, wall, &mut timing)?;
        }
        labels::set(timing.slowest.map(|(_, label)| label));
        #[cfg(test)]
        self.audit_keys(now);
        Ok(())
    }

    /// A native job's fields, read through `node` (`Serve::read_fields`); whether it is over.
    fn read_fields<N: Serve>(&mut self, node: &mut N, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        node.read_fields(job)
    }

    /// An operator's request at its turn: its one command or read of the root range asked, once;
    /// a key's id and secret drawn and the secret wrapped under the root key's current generation
    /// first, once, so a turn without room asks the same key again.
    fn step_admin<N: Serve>(&mut self, node: &mut N, at: usize) -> Result<bool, NodeError> {
        let Some(Job {
            work: Work::Admin(admin),
            pending,
            ..
        }) = self.jobs.get(at)
        else {
            return Ok(true);
        };
        if !pending.is_empty() || admin.asked {
            return Ok(false);
        }
        if let Admin::Key {
            principal,
            rotating,
        } = &admin.asking
        {
            let made = self.issue(principal, *rotating);
            let Some(Job {
                work: Work::Admin(admin),
                ..
            }) = self.jobs.get_mut(at)
            else {
                return Ok(true);
            };
            match made {
                Ok((command, issued)) => {
                    admin.asking = Admin::Command(Box::new(command));
                    admin.issued = Some(issued);
                }
                Err(status) => {
                    self.fail_job(node, at, status)?;
                    return self.drain_job(at);
                }
            }
        }
        let Some(group) = node.group_of(Layer::Root) else {
            self.fail_job(node, at, Status::Failed)?;
            return self.drain_job(at);
        };
        let asking = match self.jobs.get(at) {
            Some(Job {
                work: Work::Admin(admin),
                ..
            }) => match &admin.asking {
                Admin::Command(command) => Ok(Command::Root(command.as_ref().clone())),
                Admin::Read { query, .. } => Err(query.clone()),
                Admin::Key { .. } => return Ok(false),
            },
            _ => return Ok(true),
        };
        let asked = match asking {
            Ok(command) => node.ask(JobAsk::Command(group, command)),
            Err(query) => node.ask(JobAsk::Read(group, query)),
        };
        match asked {
            Ok(pending) => {
                if let Some(Job {
                    work: Work::Admin(admin),
                    ..
                }) = self.jobs.get_mut(at)
                {
                    admin.asked = true;
                    self.wait_on(at, pending, None);
                }
                Ok(false)
            }
            // Asked again at the next turn, as every ask the node has no room for is.
            Err(NodeError::Asks) => Ok(false),
            Err(_) => {
                self.fail_job(node, at, Status::Failed)?;
                self.drain_job(at)
            }
        }
    }

    /// A key for `principal`: its id and secret drawn from the OS, and the command that makes it,
    /// its secret wrapped under the root key's current generation, the last the node holds
    /// (encryption.md §2); with the key, to reply once the root range made it.
    fn issue(
        &self,
        principal: &str,
        rotating: bool,
    ) -> Result<(root::Command, (String, mantle_s3::account::Secret)), Status> {
        let (generation, key) = self
            .gateway
            .as_deref()
            .and_then(|g| g.keys.last())
            .ok_or(Status::Failed)?;
        let id = mantle_s3::account::draw_id().map_err(|_| Status::Failed)?;
        let secret = mantle_s3::account::draw_secret().map_err(|_| Status::Failed)?;
        let wrapped = mantle_s3::seal::wrap_secret(&secret, key).map_err(|_| Status::Failed)?;
        let command = root::Command::Principal(principal::Command::Key(principal::NewKey {
            principal: principal.to_owned(),
            id: id.clone(),
            generation: *generation,
            wrapped,
            rotating,
            at_ns: 0,
        }));
        Ok((command, (id, secret)))
    }

    /// An operator's request answered by the root range: its reply, IAM's error codes for its
    /// refusals (research/91 §2), a key made with its id and secret, and a page of principals.
    fn admin_answered<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        outcome: Outcome,
    ) -> Result<(), NodeError> {
        let one = matches!(
            self.jobs.get(at),
            Some(Job {
                work: Work::Admin(admin),
                ..
            }) if matches!(admin.asking, Admin::Read { one: true, .. })
        );
        let reply = match outcome {
            Outcome::Command(CommandOutcome::Answered(Answer::Root(root::Outcome::Principal(
                made,
            )))) => match made {
                principal::Outcome::Made
                | principal::Outcome::Set
                | principal::Outcome::Removed => Reply::status(Status::Done),
                principal::Outcome::KeyMade { replacing } => {
                    let issued = match self.jobs.get_mut(at) {
                        Some(Job {
                            work: Work::Admin(admin),
                            ..
                        }) => admin.issued.take(),
                        _ => None,
                    };
                    match issued {
                        Some((id, secret)) => Reply {
                            listing: Some(Listing::Issued(Box::new(Issued {
                                id,
                                secret,
                                replacing,
                            }))),
                            ..Reply::status(Status::Done)
                        },
                        None => Reply::status(Status::Failed),
                    }
                }
                principal::Outcome::Taken => Reply::status(Status::EntityAlreadyExists),
                principal::Outcome::Full => Reply::status(Status::LimitExceeded),
                principal::Outcome::Unknown => Reply::status(Status::NoSuchEntity),
                principal::Outcome::Held => Reply::status(Status::DeleteConflict),
                principal::Outcome::Unrotatable => Reply::status(Status::NothingToRotate),
                principal::Outcome::Invalid => Reply::status(Status::InvalidArgument),
            },
            Outcome::Read(ReadOutcome::Read(read)) => match *read {
                Read::Principals(page) if one && page.principals.is_empty() => {
                    Reply::status(Status::NoSuchEntity)
                }
                Read::Principals(page) => Reply {
                    listing: Some(Listing::Principals(page)),
                    ..Reply::status(Status::Done)
                },
                _ => Reply::status(Status::Failed),
            },
            _ => Reply::status(Status::Failed),
        };
        self.reply_job(node, at, reply)
    }

    /// Job `at`'s step at the turn of `now`, the wall clock read `wall` for it, timed in `timing`
    /// where it did work; whether it is over, and so let go. Bytes of its own body taken or of its
    /// reply given are its work moving, as its asks answered are: a native job counts them in its
    /// door, taken here as a value into the node's count (node.md §2.9 step 6c). One not over
    /// rests where it waits on its own asks alone (`Job::rests`).
    fn turn_job<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        now: u64,
        wall: u64,
        timing: &mut Timing,
    ) -> Result<bool, NodeError> {
        let before = (node.moved(), self.moved);
        let (label, asks) = self
            .jobs
            .get(at)
            .map_or((None, 0), |job| (job.label, job.pending.len()));
        let over = self.step_job(node, at, wall)?;
        if let Some(Job {
            door: Door::Native(_, native),
            ..
        }) = self.jobs.get_mut(at)
        {
            self.moved = self.moved.saturating_add(std::mem::take(&mut native.moved));
        }
        let moved = (node.moved(), self.moved) != before;
        let asked = self
            .jobs
            .get(at)
            .is_some_and(|job| job.pending.len() != asks);
        if moved && let Some(job) = self.jobs.get_mut(at) {
            job.moved_ns = now;
        }
        if moved || asked || over {
            timing.worked(label);
        }
        // What it still waits on is let go: an ended get's chunks still out, say; and its door's
        // half goes with its end, told first that its job let go, so the node's task writes
        // nothing past what it gave wherever the half is kept: here, once its reply's half is back
        // too, where the node's task holds that half (`Serve::job_ended`).
        if over
            && let Some(Job {
                door,
                admitted_ns,
                charge,
                driven,
                pending,
                ..
            }) = self.remove(at)
        {
            let half = match door {
                Door::Native(_, native) => {
                    native
                        .handed
                        .ended
                        .store(true, std::sync::atomic::Ordering::Release);
                    Some(native)
                }
                Door::Local(_) => None,
            };
            let asks = pending.into_iter().map(|(pending, _)| pending);
            if let Some(native) = node.job_ended(admitted_ns, charge, driven, asks, half) {
                self.doors.give_native(native);
            }
        }
        // Its keys from its state as its turn ends; one over is let go, and the job moved into its
        // slot keyed as its own turn ended, or at its turn next in the step.
        if !over {
            self.rest(at);
            self.rekey(at, now);
        }
        Ok(over)
    }

    /// Asks again each chunk a put's or a get's driver holds for want of room, its volume's or the
    /// transport's, of the same volume; a driver that fails doing so ends its job as its failed
    /// answers do. A job past its client's patience is offered none: `bound_jobs` ends it.
    ///
    /// Only the jobs waiting are offered it (`rooms`), the first to wait first: a turn costs by
    /// the waiters, not the jobs held, and the room freed goes to those that waited longest, as
    /// a first-come queue serves.
    fn offer_room<N: Serve>(&mut self, node: &mut N, now: u64) -> Result<(), NodeError> {
        let mut offered = std::mem::take(&mut self.offered);
        offered.clear();
        offered.extend_from_slice(self.rooms.entries());
        offered.sort_unstable();
        for &(_, at) in &offered {
            #[cfg(test)]
            tests::READS.with(|reads| reads.set(reads.get().saturating_add(1)));
            let Some(job) = self.jobs.get(at) else {
                continue;
            };
            if job.bounded(now) != Bounded::Held || !job.waits_for_room() {
                continue;
            }
            // Room offered to a chunk that waits for it is its job's work, as an answer is.
            self.wake(at);
            let Some(job) = self.jobs.get_mut(at) else {
                continue;
            };
            let (put, get) = match &mut job.work {
                Work::Put(putting) => (putting.put.as_mut(), None),
                Work::Get(getting) => (None, getting.get.as_mut()),
                Work::Copy(copying) => (copying.put.as_mut(), copying.get.as_mut()),
                _ => (None, None),
            };
            // A driver that fails keeps its error as its outcome, which its job answers: a get's
            // at its next turn (`write_get`, `step_copy`).
            if let Some(get) = get.filter(|get| get.waits_for_room()) {
                let _ = get.room();
            }
            if let Some(put) = put
                && put.waits_for_room()
                && put.room().is_err()
                && put.outcome().is_none()
            {
                self.fail_job(node, at, Status::Failed)?;
            }
        }
        self.offered = offered;
        Ok(())
    }

    /// Each operation whose work is held, by an ask of its own out (a peer's transfer its
    /// transport keeps, or a write at this node's device) or a chunk waiting for room, held to its
    /// client's patience (research/77 D2): one past it is answered `SlowDown` and lets go of what
    /// it asked, its client gone or going; one within it tells its asker its work is held. A get
    /// replied before its body: its asks, chunk reads, are given up by its transport once their
    /// peers say nothing, and it ends unfinished as a get does; one whose chunk read waits for room
    /// is the node's own wait, which no transport ends, and its driver ends `Patience`. An
    /// operation waiting on its client, for its body or room to write its reply, is the
    /// transport's to judge, and a native one whose own bytes move tells its asker so as they
    /// move (`Job::moving`).
    ///
    /// Only the jobs due are read (`beats`, `Job::beat_ns`), each keyed again as it is bounded:
    /// a turn costs by the bounds and vouches due, not the jobs held. Nothing here ends a job, so
    /// each slot due names its job throughout.
    fn bound_jobs<N: Serve>(&mut self, node: &mut N, now: u64, beat: u64) -> Result<(), NodeError> {
        let mut due = std::mem::take(&mut self.due);
        self.beats.due(now, &mut due);
        for &at in &due {
            self.bound_job(node, at, now, beat)?;
            self.rekey(at, now);
        }
        self.due = due;
        Ok(())
    }

    /// Job `at` bounded at `now` ([`Host::bound_jobs`]).
    fn bound_job<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        now: u64,
        beat: u64,
    ) -> Result<(), NodeError> {
        #[cfg(test)]
        tests::READS.with(|reads| reads.set(reads.get().saturating_add(1)));
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(());
        };
        if !job.working() {
            return Ok(());
        }
        if !job.held() {
            job.moving(now, beat);
            return Ok(());
        }
        match job.bounded(now) {
            Bounded::SlowDown => self.slow_job(node, at, door::Slowed::Patience)?,
            // The job says so on its door, twice a beat or as its work moves, and the node's
            // task tells its exchange's asker (`Replies::write`).
            Bounded::Held => job.vouch(now, beat),
            // A chunk read waiting for room is the node's own wait, which no transport ends:
            // its get ends there, unfinished, as one whose driver fails past its reply's head
            // does (`write_get`).
            Bounded::Unvouched if job.waits_for_room() => {
                if let Some(get) = self.get_of(at) {
                    get.out_of_patience();
                }
            }
            Bounded::Unvouched => {}
        }
        Ok(())
    }

    /// One operation's turn, at the wall clock's `wall`; whether it is over.
    fn step_job<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        wall: u64,
    ) -> Result<bool, NodeError> {
        #[cfg(test)]
        tests::STEPS.with(|steps| steps.set(steps.get().saturating_add(1)));
        let Some(job) = self.jobs.get(at) else {
            return Ok(true);
        };
        match job.work {
            Work::Fields(..) => self.read_fields(node, at),
            // Taken out after the host's step, by the node's task that reads its fields.
            Work::Handing(_) => Ok(false),
            Work::Draining(_) => self.drain_job(at),
            Work::Answering(_) => self.answer_door(at),
            Work::Bucket(_) => self.step_bucket(node, at),
            Work::Put(_) => self.step_put(node, at, wall),
            Work::Get(_) => self.step_get(node, at),
            Work::Command(_) => self.step_command(node, at),
            Work::Complete(_) => self.step_complete(node, at),
            Work::Read(_) => self.step_single(node, at),
            Work::Deletes(_) => self.step_deletes(node, at),
            Work::Copy(_) => self.step_copy(node, at, wall),
            Work::Admin(_) => self.step_admin(node, at),
        }
    }

    /// The outcomes of what job `at` asked, taken from the node's answers and given to it at the
    /// turn of `now`.
    fn take_answers<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        now: u64,
    ) -> Result<(), NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(());
        };
        if job.pending.is_empty() {
            return Ok(());
        }
        // Taken out while the node gives each its outcome, and given back with those still out,
        // each answered let go of its slot.
        let mut pending = std::mem::take(&mut job.pending);
        let mut answered: Vec<(Option<u64>, Outcome)> = Vec::new();
        let asks = &mut self.asks;
        pending.retain(|(asked, request)| match node.outcome(*asked) {
            Some(outcome) => {
                asks.release(Some(*asked));
                answered.push((*request, outcome));
                false
            }
            None => true,
        });
        if let Some(job) = self.jobs.get_mut(at) {
            job.pending = pending;
        }
        #[cfg(test)]
        self.audit();
        for (request, outcome) in answered {
            self.answer_job(node, at, request, outcome, now)?;
        }
        Ok(())
    }

    /// Gives job `at` an outcome of what it asked, at the turn of `now`.
    fn answer_job<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        request: Option<u64>,
        outcome: Outcome,
        now: u64,
    ) -> Result<(), NodeError> {
        // An answer is a resting job's work: its next turn takes it on.
        self.wake(at);
        // A range step that waited its range's budget without a leader that can serve it (node.md
        // §5.4), the only end a job's steps have: the request is answered `SlowDown`, a cause that
        // passes, which clients retry.
        let leaderless = matches!(
            outcome,
            Outcome::Read(ReadOutcome::Deadline) | Outcome::Command(CommandOutcome::Deadline)
        );
        if leaderless && self.jobs.get(at).is_some_and(Job::working) {
            return self.slow_job(node, at, door::Slowed::Leaderless);
        }
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(());
        };
        job.answered(&outcome, now);
        let principal = job.principal.clone();
        match &mut job.work {
            Work::Fields(..) | Work::Handing(_) | Work::Draining(_) | Work::Answering(_) => {}
            // A native upload opened: its ID, and its progress, every run but the last as long
            // as the plan made it.
            Work::Command(commanding) if matches!(commanding.then, Then::Open { .. }) => {
                let Then::Open { run, .. } = commanding.then else {
                    return Ok(());
                };
                return match outcome {
                    Outcome::Read(ReadOutcome::Read(read)) => self.command_read(node, at, *read),
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
                        self.reply_job(node, at, reply)
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(refused))) => {
                        self.fail_job(node, at, name_status(&refused))
                    }
                    _ => self.fail_job(node, at, Status::Failed),
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
                            Some(status) => self.fail_job(node, at, status),
                            None => Ok(()),
                        };
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Bucket(o))) => {
                        if coordinating.coordinator.is_none() {
                            return self.begun(node, at, o);
                        }
                        coordinator::Answer::Bucket(o)
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => {
                        coordinator::Answer::Name(o)
                    }
                    Outcome::Read(ReadOutcome::Read(read)) => match *read {
                        Read::Gate(gate) => coordinator::Answer::Gate(gate),
                        Read::Probe(probe) => coordinator::Answer::Probe(probe),
                        _ => return self.fail_job(node, at, Status::Failed),
                    },
                    _ => return self.fail_job(node, at, Status::Failed),
                };
                let Some(c) = coordinating.coordinator.as_mut() else {
                    return self.fail_job(node, at, Status::Failed);
                };
                if c.answer(answer).is_err() {
                    return self.fail_job(node, at, Status::Failed);
                }
            }
            Work::Put(putting) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.bucket_read(node, at, *read);
                }
                (None, _) => return self.fail_job(node, at, Status::Failed),
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
                    return self.current_read(node, at, *read);
                }
                (None, _) => return self.fail_job(node, at, Status::Failed),
                (Some(id), outcome) => {
                    // An answer the driver refuses ends it with that outcome, which its next
                    // turn ends the exchange on. A block it read is timed for the window the
                    // node's next gets hold (`crate::window`).
                    if let Some(get) = getting.get.as_mut() {
                        get.tick(now);
                        let _ = get.answer(id, get_answer(outcome));
                        if let Some(ns) = get.take_read() {
                            node.read(ns);
                        }
                    }
                }
            },
            Work::Command(_) => {
                return match outcome {
                    Outcome::Read(ReadOutcome::Read(read)) => self.command_read(node, at, *read),
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
                        self.reply_job(node, at, reply)
                    }
                    Outcome::Command(CommandOutcome::Answered(Answer::Bucket(o))) => {
                        let status = match o {
                            bucket::Outcome::Versioned => Status::Done,
                            bucket::Outcome::VersioningLocked => Status::InvalidBucketState,
                            bucket::Outcome::NoSuchBucket => Status::NoSuchBucket,
                            bucket::Outcome::OperationAborted => Status::OperationAborted,
                            _ => Status::Failed,
                        };
                        self.fail_job(node, at, status)
                    }
                    _ => self.fail_job(node, at, Status::Failed),
                };
            }
            Work::Complete(completing) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.complete_read(node, at, *read);
                }
                (None, _) => return self.fail_job(node, at, Status::Failed),
                (Some(id), outcome) => {
                    // A read or command refused, or answered with another kind: `500`, which
                    // clients retry; never the 404 an upload not found would be.
                    let Some(answer) = complete_answer(outcome) else {
                        return self.fail_job(node, at, Status::Failed);
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
                    Outcome::Read(ReadOutcome::Read(read)) => self.single_read(node, at, *read),
                    _ => self.fail_job(node, at, Status::Failed),
                };
            }
            Work::Deletes(deleting) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.deletes_read(node, at, *read);
                }
                (None, _) => return self.fail_job(node, at, Status::Failed),
                (Some(index), outcome) => {
                    let index = usize::try_from(index).unwrap_or(usize::MAX);
                    deleting.out = deleting.out.saturating_sub(1);
                    let (Some(target), Some(slot)) =
                        (deleting.objects.get(index), deleting.results.get_mut(index))
                    else {
                        return self.fail_job(node, at, Status::Failed);
                    };
                    *slot = Some(match outcome {
                        Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => {
                            deleted(target, o)
                        }
                        // A command its range answered with another kind: S3's `500`, which
                        // its client asks again.
                        _ => Deleted {
                            key: target.key.clone(),
                            version: target.version.clone(),
                            status: Status::Failed,
                            marker: None,
                        },
                    });
                }
            },
            Work::Copy(copying) => match (request, outcome) {
                (None, Outcome::Read(ReadOutcome::Read(read))) => {
                    return self.copy_read(node, at, *read);
                }
                (None, _) => return self.fail_job(node, at, Status::Failed),
                (Some(tag), outcome) => match untagged(tag) {
                    // An answer the driver refuses ends it with that outcome, which its next
                    // turn replies. A block its get read is timed as a get's is, for the windows
                    // the node's next gets and copies hold (`crate::window`).
                    (Driver::Get, id) => {
                        if let Some(get) = copying.get.as_mut() {
                            get.tick(now);
                            let _ = get.answer(id, get_answer(outcome));
                            if let Some(ns) = get.take_read() {
                                node.read(ns);
                            }
                        }
                    }
                    (Driver::Put, id) => {
                        if let Some(put) = copying.put.as_mut() {
                            let _ = put.answer(id, put_answer(outcome));
                        }
                    }
                },
            },
            Work::Admin(_) => return self.admin_answered(node, at, outcome),
        }
        Ok(())
    }

    /// A bucket's create or delete answered by the Bucket range's first command: its
    /// coordinator, or the reply it gets.
    fn begun<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        outcome: bucket::Outcome,
    ) -> Result<(), NodeError> {
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
            return self.fail_job(node, at, status);
        }
        let Some(gateway) = self.gateway.as_deref() else {
            return self.fail_job(node, at, Status::Failed);
        };
        let bounds = coordinator::Bounds {
            budget: gateway.scan_rows,
            ranges: node.ranges(),
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
            _ => self.fail_job(node, at, Status::Failed),
        }
    }

    /// A bucket's create or delete at a turn: a delete's bucket row read first, its owner checked;
    /// then its first command; then its coordinator's next request, one at a time; once the
    /// coordinator is done, its answer.
    fn step_bucket<N: Serve>(&mut self, node: &mut N, at: usize) -> Result<bool, NodeError> {
        let quota = self.gateway.as_deref().map_or(0, |g| g.bucket_quota);
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
            (node.group_of(Layer::Bucket), node.group_of(Layer::Name))
        else {
            self.fail_job(node, at, Status::Failed)?;
            return self.drain_job(at);
        };
        let asked = match next {
            Next::Done(status) => {
                self.fail_job(node, at, status)?;
                return self.drain_job(at);
            }
            Next::Owner => node.ask(JobAsk::Read(bucket_group, Query::Bucket { bucket })),
            Next::Request(coordinator::Request::Bucket(command)) => {
                node.ask(JobAsk::Command(bucket_group, Command::Bucket(command)))
            }
            Next::Request(coordinator::Request::Name { command, .. }) => {
                node.ask(JobAsk::Command(name_group, Command::Name(command)))
            }
            Next::Request(coordinator::Request::Gate { generation, .. }) => {
                node.ask(JobAsk::Read(name_group, Query::Gate { bucket, generation }))
            }
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
                node.ask(JobAsk::Read(name_group, query))
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
                    self.fail_job(node, at, Status::Failed)?;
                }
                return Ok(false);
            }
        };
        self.asked(node, at, asked, None)
    }

    /// What job `at` asked, kept to wait on; the asking's failure ends the job. An ask the node
    /// has no room for is asked again at the next turn.
    fn asked<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        asked: Result<Pending, NodeError>,
        request: Option<u64>,
    ) -> Result<bool, NodeError> {
        match asked {
            Ok(pending) => {
                self.wait_on(at, pending, request);
                Ok(false)
            }
            Err(NodeError::Asks) => Ok(false),
            Err(_) => {
                self.fail_job(node, at, Status::Failed)?;
                self.drain_job(at)
            }
        }
    }

    /// A put's bucket row, read: its driver made over it, or the reply `NoSuchBucket`. A native
    /// upload's run asks its upload's row next, which it is checked against (`run_read`).
    fn bucket_read<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        read: Read,
    ) -> Result<(), NodeError> {
        let read = match read {
            Read::Upload(upload) => return self.run_read(node, at, upload),
            read => read,
        };
        let Read::Bucket(Some(row)) = read else {
            return self.fail_job(node, at, Status::NoSuchBucket);
        };
        if row.state != BucketState::Active {
            return self.fail_job(node, at, Status::NoSuchBucket);
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
            let Some(group) = node.group_of(Layer::Name) else {
                return self.fail_job(node, at, Status::Failed);
            };
            let asked = node.ask(JobAsk::Read(group, query));
            return self.asked(node, at, asked, None).map(|_| ());
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
                    headers: std::mem::take(&mut putting.metadata),
                    retention: None,
                    legal_hold: None,
                    listing: None,
                },
                default,
                id: putting.id,
                deadline_ns: 0,
            }),
        };
        self.start_put(node, at, commit)
    }

    /// A native upload's run, its upload's row read (gateway.md §2.1): begun at the offset the
    /// row holds, as long as the plan's runs or, the last, no longer, numbered the next; or the
    /// reply it earns, one at any other offset or after the last run `Offset`, naming the
    /// upload's.
    fn run_read<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        upload: Option<Upload>,
    ) -> Result<(), NodeError> {
        let Some(Job {
            work: Work::Put(putting),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let Some(upload) = upload else {
            return self.fail_job(node, at, Status::NoSuchUpload);
        };
        // An upload CreateMultipartUpload made takes its client's parts, never runs.
        let (Some(runs), Some(appending)) = (upload.runs, putting.run.as_mut()) else {
            return self.fail_job(node, at, Status::InvalidRequest);
        };
        appending.runs = Some(runs);
        if runs.ended || appending.offset != runs.offset {
            let reply = Reply {
                progress: Some(Box::new(progress(&runs, None))),
                ..Reply::status(Status::Offset)
            };
            return self.reply_job(node, at, reply);
        }
        let (offset, last) = (appending.offset, appending.last);
        let Some(incarnation) = appending.incarnation else {
            return self.fail_job(node, at, Status::Failed);
        };
        // The run's checksum is in the algorithm its object is held to at its end.
        if putting.trailing.map(Algorithm::code) != upload.checksum.map(|(code, _)| code) {
            return self.fail_job(node, at, Status::InvalidRequest);
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
            return self.fail_job(node, at, Status::InvalidArgument);
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
        self.start_put(node, at, commit)
    }

    /// A put's driver, made over `commit`: its file's ID and data key drawn, under the root key's
    /// current generation, and its blocks laid out by the scheme new blocks take; or the reply
    /// its failure earns.
    fn start_put<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        commit: Commit,
    ) -> Result<(), NodeError> {
        let Ok(Some(scheme)) = node.table().and_then(Table::scheme) else {
            return self.fail_job(node, at, Status::Failed);
        };
        let now = node.wall_ns();
        let Some(gateway) = self.gateway.as_deref() else {
            return self.fail_job(node, at, Status::Failed);
        };
        let Some((generation, root)) = gateway.keys.last() else {
            return self.fail_job(node, at, Status::Failed);
        };
        let holding = Holding {
            handover_ns: gateway.handover_ns,
            window: self.jobs.get(at).map_or(gateway.window, |job| job.window),
        };
        let made = (|| {
            let data = DataKey::generate().ok()?;
            let wrapped = WrappedKey {
                by: Wrapper::Root(*generation),
                bytes: data.wrap(root).ok()?,
            };
            // The file's ID from the thread's generator, as its blocks' are (`put::id`).
            let keys = Keys {
                file: put::id().ok()?,
                data,
                wrapped,
            };
            Some((keys, Layout::new(scheme).ok()?))
        })();
        let Some((keys, layout)) = made else {
            return self.fail_job(node, at, Status::Failed);
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
            Err(error) => self.reply_job(node, at, put_failed(&error)),
        }
    }

    /// A put's turn: its bucket's row read first; then its body read as far as the driver takes
    /// it, its driver's time moved and its requests asked; once its outcome is known, its answer.
    fn step_put<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        wall: u64,
    ) -> Result<bool, NodeError> {
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
                        Err(error) => put_failed(error),
                    });
                    // What its blocks took to be recorded and its body to come, for the windows the
                    // node's next puts hold (`crate::window`).
                    if let Some(ns) = put.take_committed() {
                        node.committed(ns);
                    }
                    if let Some((bytes, ns)) = put.take_filled() {
                        node.filled(bytes, ns);
                    }
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
                let Some(group) = node.group_of(Layer::Bucket) else {
                    self.fail_job(node, at, Status::Failed)?;
                    return self.drain_job(at);
                };
                let asked = node.ask(JobAsk::Read(group, Query::Bucket { bucket }));
                return self.asked(node, at, asked, None);
            }
            Step::Go => {}
        }
        self.feed_put(node, at)?;
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
            if let Some(request) = self.ask_put(node, at, id, request, wall)? {
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

    /// Reads a put's body into its driver as far as the driver takes it, from its door's channel
    /// ([`take_body`]): a local door's as the S3 listener gives it, the listener told once the
    /// driver first wants a byte, so a client that waits for `100 Continue` sends none before every
    /// check that could refuse it passed, and each piece a sample of the body's depth
    /// (`door::Depth`); a native exchange's as the node's task gives it ([`Feeding`]). A job whose
    /// body does not hold ends with the status that says why.
    fn feed_put<N: Serve>(&mut self, node: &mut N, at: usize) -> Result<(), NodeError> {
        let Self { jobs, moved, .. } = self;
        let Some(job) = jobs.get_mut(at) else {
            return Ok(());
        };
        let Work::Put(putting) = &mut job.work else {
            return Ok(());
        };
        let Some(put) = putting.put.as_ref() else {
            return Ok(());
        };
        let fed = match &mut job.door {
            Door::Local(local) => {
                if !local.ready && put.wants_body() {
                    local.ready = true;
                    if local.offer(door::Answer::Ready) == Sent::Gone {
                        return self.fail_job(node, at, Status::IncompleteBody);
                    }
                }
                match local.body.as_mut() {
                    Some(body) => take_body(body, None, putting, moved, |paced| node.took(paced)),
                    // Let go of as its job answered, after which no put of it takes a piece.
                    None => Err(Status::IncompleteBody),
                }
            }
            // A native put's pieces come as its exchange delivers them, at no connection's pace,
            // and their bytes are counted in its door. Its fields are read before its put is made,
            // which takes its body.
            // A piece taken frees the channel's room and the storage the node's task reads the
            // next into, so the job names its exchange for its feeding's visit (`Native::mark`) and
            // a job on a driving shard turns the node's task: once `take_body` returns, every piece
            // it took let go, as the node's task reads the next only into storage no piece holds
            // (`Feeding::free`).
            Door::Native(_, native) => {
                let Native {
                    body,
                    moved,
                    handed,
                    ..
                } = &mut **native;
                let mut took = false;
                let fed = take_body(body, Some(&handed.cut), putting, moved, |_| took = true);
                if took {
                    native.mark();
                    #[cfg(test)]
                    native
                        .handed
                        .took
                        .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    native.wake_node();
                }
                fed
            }
        };
        match fed {
            Ok(()) => Ok(()),
            Err(status) => self.fail_job(node, at, status),
        }
    }

    /// Asks a put's driver's request of what serves it at the turn's wall clock, `wall`; the
    /// request back when the node holds as many asks as it keeps.
    fn ask_put<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        id: u64,
        request: put::Request,
        wall: u64,
    ) -> Result<Option<put::Request>, NodeError> {
        let Some(kept) = self.kept_id(at, Driver::Put, id) else {
            self.fail_job(node, at, Status::Failed)?;
            return Ok(None);
        };
        // A command goes to its range as the driver made it, moved, once the node has room
        // for it; without room it waits whole for the next turn.
        let request = match request {
            put::Request::Block(command) if node.command_room() => {
                return self.ask_command(node, at, kept, Layer::Block, Command::Block(command));
            }
            put::Request::File(command) if node.command_room() => {
                return self.ask_command(node, at, kept, Layer::File, Command::File(command));
            }
            put::Request::Name(command) if node.command_room() => {
                return self.ask_command(node, at, kept, Layer::Name, Command::Name(command));
            }
            request => request,
        };
        let asked = match &request {
            put::Request::Place { block, .. } => {
                // Only volumes that take chunks now, their nodes seen, weighted by measured
                // availability (docs/design/repair.md §5), at the turn's wall clock.
                let Ok(volumes) = node
                    .table()
                    .map(|t| place::offer(*block, &t.facts, &[], false, wall))
                else {
                    self.fail_job(node, at, Status::Failed)?;
                    return Ok(None);
                };
                // Refused, the driver ends with that outcome, which its next turn replies.
                if let Some(put) = self.put_of(at) {
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
                let Ok(holder) = node.table().map(|t| t.node_of(*volume)) else {
                    self.fail_job(node, at, Status::Failed)?;
                    return Ok(None);
                };
                let Some(holder) = holder else {
                    // A volume the table no longer holds refuses it: the next is asked.
                    if let Some(put) = self.put_of(at) {
                        let _ = put.answer(id, put::Answer::Refused { full: false });
                    }
                    return Ok(None);
                };
                let remaining = self.remaining_of(at);
                // The replicas' one copy of the bytes, shared (a count, not a copy): each
                // replica's put writes or sends from it.
                node.ask(JobAsk::Put {
                    at: holder,
                    volume: *volume,
                    key: *key,
                    bytes: bytes.clone(),
                    crc32c: *crc,
                    remaining_ns: remaining,
                })
            }
            put::Request::Block(_) | put::Request::File(_) | put::Request::Name(_) => {
                Err(NodeError::Asks)
            }
        };
        match asked {
            Ok(pending) => {
                self.wait_on(at, pending, Some(kept));
                Ok(None)
            }
            Err(NodeError::Asks) => Ok(Some(request)),
            Err(_) => {
                self.fail_job(node, at, Status::Failed)?;
                Ok(None)
            }
        }
    }

    /// Asks `command` of `layer`'s range for request `id` of job `at`, the node having room for
    /// it; a refusal fails the job.
    fn ask_command<R, N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        id: u64,
        layer: Layer,
        command: Command,
    ) -> Result<Option<R>, NodeError> {
        let asked = node
            .group_of(layer)
            .ok_or(NodeError::Bounds(
                "no range of the command's layer here".into(),
            ))
            .and_then(|g| node.ask(JobAsk::Command(g, command)));
        match asked {
            Ok(pending) => self.wait_on(at, pending, Some(id)),
            Err(_) => self.fail_job(node, at, Status::Failed)?,
        }
        Ok(None)
    }

    /// A get's or head's current version, read: its preconditions judged and its range resolved
    /// against it (05 §2.4, §12); then a head's facts replied, or a get's driver made over the
    /// range and the reply's head sent.
    fn current_read<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        read: Read,
    ) -> Result<(), NodeError> {
        let (order, version) = match read {
            Read::NoBucket => return self.fail_job(node, at, Status::NoSuchBucket),
            Read::Current(None) => return self.fail_job(node, at, Status::NoSuchKey),
            Read::Current(Some(found)) => found,
            Read::Version(None) => return self.fail_job(node, at, Status::NoSuchVersion),
            // A named version that is a delete marker: "the response returns a `405 Method Not
            // Allowed` error and the `Last-Modified: timestamp` response header" (research/89
            // §2.1), the marker's facts with it.
            Read::Version(Some((order, marker))) if marker.marker => {
                let reply = facts(Status::MethodNotAllowed, order, &marker);
                return self.reply_job(node, at, reply);
            }
            Read::Version(Some(found)) => found,
            _ => return self.fail_job(node, at, Status::Failed),
        };
        let Some(gateway) = self.gateway.as_deref() else {
            return self.fail_job(node, at, Status::Failed);
        };
        let keyring = RootKeys {
            keys: gateway
                .keys
                .iter()
                .map(|(g, key)| (*g, *key.bytes()))
                .collect(),
        };
        let Some(Job {
            work: Work::Get(getting),
            window,
            charge,
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
            return self.reply_job(node, at, reply);
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
                return self.reply_job(node, at, refused);
            }
        };
        if getting.head {
            return self.reply_job(node, at, reply);
        }
        let length = served.end.saturating_sub(served.start);
        // One GET returns at most S3's single GET; an object past it is read in ranges (05 §4.1).
        if length > mantle_s3::body::MAX_GET {
            return self.fail_job(node, at, Status::EntityTooLarge);
        }
        // Its window, now its range's length is known: the one learned, as wide as its blocks and
        // the operations' bytes beside the others' hold, the bytes past its admitted charge taken.
        let learned = node.windows();
        let held = reading(gateway, &learned.get, length, *charge, |more| {
            node.holds(more)
        });
        let more = gateway.charges.read(held).saturating_sub(*charge);
        match Get::new(version.file, size, served, Box::new(keyring), held) {
            Ok(mut get) => {
                get.tick(hyper_rt::futures::now_ns());
                getting.get = Some(get);
            }
            Err(_) => return self.fail_job(node, at, Status::Failed),
        }
        // The reply waits for the range's first bytes (`write_get`).
        getting.total = length;
        getting.held = Some(Box::new(reply));
        *window = held;
        *charge = charge.saturating_add(more);
        node.recharge(more, 0);
        Ok(())
    }

    /// Sends a get's held reply once its driver has the range's first bytes or its end: its head,
    /// and natively its fields opening the body, ahead of the range's bytes. A driver that failed
    /// before is answered with the refusal its failure is (`get_status`), no byte sent. Whether the
    /// job is over.
    fn send_held<N: Serve>(&mut self, node: &mut N, at: usize) -> Result<Option<bool>, NodeError> {
        let bound_ns = self.jobs.get(at).map_or(0, Job::bound_ns);
        let Some(Job {
            work: Work::Get(getting),
            door,
            admitted_ns,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(Some(true));
        };
        let Some(get) = getting.get.as_ref() else {
            return Ok(Some(false));
        };
        if getting.held.is_none() {
            return Ok(None);
        }
        if let Some(Err(e)) = get.outcome() {
            let failed = get_failed(e);
            getting.held = None;
            self.reply_job(node, at, failed)?;
            return Ok(Some(false));
        }
        if !get.ready() && get.outcome().is_none() {
            return Ok(Some(false));
        }
        let Some(reply) = getting.held.take() else {
            return Ok(None);
        };
        // The reply goes first, its bytes after it as the driver gives them.
        let sent = match door {
            Door::Local(local) => local.offer(door::Answer::Head(reply)),
            // Natively its fields open the body, ahead of the range's bytes, which the head's
            // length counts as the node's task writes it.
            Door::Native(_, native) => {
                let Ok((head, fields)) = reply.encode() else {
                    self.fail_job(node, at, Status::Failed)?;
                    return Ok(Some(false));
                };
                native.give(Told::Head(Box::new(Opening {
                    head,
                    fields: Bytes::from(fields),
                    body: getting.total,
                    slowed: None,
                    bound_ns,
                    admitted: admitted_ns.is_some(),
                })))
            }
        };
        if sent == Sent::Gone {
            // Its asker is gone: the job ends with its exchange.
            self.fail_job(node, at, Status::Failed)?;
            return Ok(Some(false));
        }
        Ok(None)
    }

    /// A get's or head's turn: its current version read first; then a get's driver's requests
    /// asked and its bytes written as far as the exchange takes them; once every byte is written,
    /// its end.
    fn step_get<N: Serve>(&mut self, node: &mut N, at: usize) -> Result<bool, NodeError> {
        let name = node.group_of(Layer::Name);
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
            let query = match getting.version {
                None => Query::Current {
                    bucket: getting.bucket.clone(),
                    key: getting.key.clone(),
                },
                Some(named) => Query::Version {
                    bucket: getting.bucket.clone(),
                    key: getting.key.clone(),
                    named,
                },
            };
            let Some(group) = name else {
                self.fail_job(node, at, Status::Failed)?;
                return self.drain_job(at);
            };
            let asked = node.ask(JobAsk::Read(group, query));
            return self.asked(node, at, asked, None);
        }
        if self.write_get(node, at)? {
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
            if let Some(request) = self.ask_get(node, at, id, request)? {
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

    /// Gives a get's bytes to its door as far as it takes them, taking more from the driver as the
    /// last is taken, then its end; whether the get is over: every byte given and its end, or its
    /// driver failed, which ends the exchange unfinished, or its door is gone.
    fn write_get<N: Serve>(&mut self, node: &mut N, at: usize) -> Result<bool, NodeError> {
        if let Some(over) = self.send_held(node, at)? {
            return Ok(over);
        }
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let Work::Get(getting) = &mut job.work else {
            return Ok(true);
        };
        let Some(get) = getting.get.as_mut() else {
            return Ok(false);
        };
        // Blocks it begins from here are timed from now (`Get::take_read`).
        get.tick(hyper_rt::futures::now_ns());
        match &mut job.door {
            // Its client's pace is the node's task's to time as it writes the exchange
            // (`Replies`, `Drain`).
            Door::Native(_, native) => {
                match give_native(native, get, &mut getting.written, getting.total) {
                    Gave::Waits => Ok(false),
                    Gave::Over => Ok(true),
                    Gave::Ending => {
                        // The last answer waits for room: the job sends it before it ends.
                        job.work = Work::Answering(VecDeque::new());
                        Ok(false)
                    }
                }
            }
            Door::Local(local) => {
                // Every piece this turn sends carries the turn, the one held from the last turn's
                // among them: the node learns the reply's depth from the time between turns its
                // connection sees (`door::Replying`).
                let turn = std::time::Instant::now();
                for answer in &mut local.pending {
                    if let door::Answer::Bytes(_, at) = answer {
                        *at = turn;
                    }
                }
                // A piece is taken from the driver only once the last is sent, so the channel holds
                // what the client has yet to read and the driver the rest of its window. Its
                // client's pace is the connection's to time (`listener::io::Drained`).
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
                    if local.offer(door::Answer::Bytes(bytes, turn)) == Sent::Gone {
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
                Ok(true)
            }
        }
    }

    /// Asks a get's driver's request of what serves it; the request back when the node holds as
    /// many asks as it keeps.
    fn ask_get<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        id: u64,
        request: get::Request,
    ) -> Result<Option<get::Request>, NodeError> {
        let asked = match &request {
            get::Request::Header { file } => node
                .group_of(Layer::File)
                .ok_or(NodeError::Bounds("no File range here".into()))
                .and_then(|g| node.ask(JobAsk::Read(g, Query::Header { file: *file }))),
            get::Request::Extents { file, offset, max } => {
                let max = u32::try_from(*max).unwrap_or(u32::MAX);
                node.group_of(Layer::File)
                    .ok_or(NodeError::Bounds("no File range here".into()))
                    .and_then(|g| {
                        let query = Query::Extents {
                            file: *file,
                            offset: *offset,
                            max,
                        };
                        node.ask(JobAsk::Read(g, query))
                    })
            }
            get::Request::Block { block } => node
                .group_of(Layer::Block)
                .ok_or(NodeError::Bounds("no Block range here".into()))
                .and_then(|g| node.ask(JobAsk::Read(g, Query::Block { block: *block }))),
            get::Request::Chunk {
                volume,
                key,
                offset,
                len,
                past_suspicion,
            } => {
                let Ok(holder) = node.table().map(|t| t.node_of(*volume)) else {
                    self.fail_job(node, at, Status::Failed)?;
                    return Ok(None);
                };
                let Some(holder) = holder else {
                    // A volume the table no longer holds, retired once no block named it: the
                    // driver's row of the block may predate its chunk's move, and another place
                    // is read, or the row read again (`get::Answer::Absent`), or the driver ends
                    // with its outcome.
                    if let Some(get) = self.get_of(at) {
                        let _ = get.answer(id, get::Answer::Absent);
                    }
                    return Ok(None);
                };
                let remaining = self.remaining_of(at);
                node.ask(JobAsk::Get {
                    at: holder,
                    volume: *volume,
                    key: *key,
                    span: (*offset, *len),
                    remaining_ns: remaining,
                    past_suspicion: *past_suspicion,
                })
            }
        };
        let Some(kept) = self.kept_id(at, Driver::Get, id) else {
            self.fail_job(node, at, Status::Failed)?;
            return Ok(None);
        };
        match asked {
            Ok(pending) => {
                self.wait_on(at, pending, Some(kept));
                Ok(None)
            }
            Err(NodeError::Asks) => Ok(Some(request)),
            Err(_) => {
                self.fail_job(node, at, Status::Failed)?;
                Ok(None)
            }
        }
    }

    /// The request ID job `at` keeps its driver's request `id` under: the ID itself, or, for a
    /// copy, the ID tagged with its driver (`tagged`); none past what a u64 holds.
    fn kept_id(&self, at: usize, driver: Driver, id: u64) -> Option<u64> {
        match self.jobs.get(at).map(|job| &job.work) {
            Some(Work::Copy(_)) => tagged(driver, id),
            _ => Some(id),
        }
    }

    /// Job `at`'s get driver: its get's, or its copy's.
    fn get_of(&mut self, at: usize) -> Option<&mut Get> {
        match self.jobs.get_mut(at).map(|job| &mut job.work) {
            Some(Work::Get(getting)) => getting.get.as_mut(),
            Some(Work::Copy(copying)) => copying.get.as_mut(),
            _ => None,
        }
    }

    /// Job `at`'s put driver: its put's, or its copy's.
    fn put_of(&mut self, at: usize) -> Option<&mut Put> {
        match self.jobs.get_mut(at).map(|job| &mut job.work) {
            Some(Work::Put(putting)) => putting.put.as_mut(),
            Some(Work::Copy(copying)) => copying.put.as_mut(),
            _ => None,
        }
    }

    /// A commanding operation's turn: its bucket's row read first, then its command; the
    /// command's answer replies (`answer_job`).
    fn step_command<N: Serve>(&mut self, node: &mut N, at: usize) -> Result<bool, NodeError> {
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
        if let Some(Job {
            work: Work::Command(commanding),
            ..
        }) = self.jobs.get_mut(at)
            && let Some(row) = commanding.row.take()
        {
            self.command_read(node, at, Read::Bucket(Some(row)))?;
            return self.moved_on(at);
        }
        let Some(Job {
            work: Work::Command(commanding),
            ..
        }) = self.jobs.get(at)
        else {
            return Ok(true);
        };
        let Some(group) = node.group_of(Layer::Bucket) else {
            self.fail_job(node, at, Status::Failed)?;
            return self.drain_job(at);
        };
        let query = Query::Bucket {
            bucket: commanding.bucket.clone(),
        };
        let asked = node.ask(JobAsk::Read(group, query));
        self.asked(node, at, asked, None)
    }

    /// A commanding operation's bucket row, read: its command asked, a delete's under the
    /// bucket's incarnation and versioning (05 §7.3), a versioning change by the bucket's owner;
    /// or the reply `NoSuchBucket` or `AccessDenied`.
    fn command_read<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        read: Read,
    ) -> Result<(), NodeError> {
        let Read::Bucket(Some(row)) = read else {
            return self.fail_job(node, at, Status::NoSuchBucket);
        };
        if row.state != BucketState::Active {
            return self.fail_job(node, at, Status::NoSuchBucket);
        }
        let (Some(bucket_group), Some(name_group)) =
            (node.group_of(Layer::Bucket), node.group_of(Layer::Name))
        else {
            return self.fail_job(node, at, Status::Failed);
        };
        let Some(Job {
            work: Work::Command(commanding),
            principal,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        // A command the node has no room for is refused before it is asked (`Node::command`),
        // its value given up: the row is kept and the command made at a later turn, where one
        // made and refused was never asked again and its job, holding no ask, had no bound.
        if !node.command_room() {
            commanding.row = Some(row);
            return Ok(());
        }
        let (group, command) = match &mut commanding.then {
            Then::Delete {
                key,
                if_match,
                named,
            } => (
                name_group,
                Command::Name(Box::new(name::Command::Delete(name::Delete {
                    bucket: commanding.bucket.clone(),
                    incarnation: row.created_ns,
                    key: key.clone(),
                    versioning: row.versioning,
                    named: *named,
                    if_match: if_match.take(),
                    at_ns: 0,
                    bypass: false,
                    owner: principal.clone(),
                }))),
            ),
            // S3 lets no other principal change a bucket's versioning unless a policy grants it.
            Then::Versioning(_) if row.owner != *principal => {
                return self.fail_job(node, at, Status::AccessDenied);
            }
            Then::Versioning(versioning) => (
                bucket_group,
                Command::Bucket(bucket::Command::Version {
                    bucket: commanding.bucket.clone(),
                    incarnation: row.created_ns,
                    versioning: *versioning,
                }),
            ),
            Then::CreateUpload {
                key,
                checksum,
                metadata,
            } => (
                name_group,
                Command::Name(Box::new(name::Command::CreateUpload(name::CreateUpload {
                    bucket: commanding.bucket.clone(),
                    incarnation: row.created_ns,
                    key: key.clone(),
                    at_ns: 0,
                    upload: Upload {
                        initiated_ns: 0,
                        owner: principal.clone(),
                        headers: std::mem::take(metadata),
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
                metadata,
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
                        None => return self.fail_job(node, at, Status::Failed),
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
                            headers: std::mem::take(metadata),
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
        let asked = node.ask(JobAsk::Command(group, command));
        self.asked(node, at, asked, None).map(|_| ())
    }

    /// A completion's turn: its bucket's row read first; then its driver's requests asked; once
    /// its outcome is known, its answer.
    fn step_complete<N: Serve>(&mut self, node: &mut N, at: usize) -> Result<bool, NodeError> {
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
            let Some(group) = node.group_of(Layer::Bucket) else {
                self.fail_job(node, at, Status::Failed)?;
                return self.drain_job(at);
            };
            let asked = node.ask(JobAsk::Read(group, query));
            return self.asked(node, at, asked, None);
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
                Err(error) => Reply::status(complete_status(error)),
            };
            self.reply_job(node, at, reply)?;
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
            if let Some(request) = self.ask_complete(node, at, id, request)? {
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
    fn complete_read<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        read: Read,
    ) -> Result<(), NodeError> {
        let Read::Bucket(Some(row)) = read else {
            return self.fail_job(node, at, Status::NoSuchBucket);
        };
        if row.state != BucketState::Active {
            return self.fail_job(node, at, Status::NoSuchBucket);
        }
        let Some(handover_ns) = self.gateway.as_deref().map(|g| g.handover_ns) else {
            return self.fail_job(node, at, Status::Failed);
        };
        let now = node.wall_ns();
        // The file's ID from the thread's generator, as a put's is (`put::id`).
        let Ok(file) = put::id() else {
            return self.fail_job(node, at, Status::Failed);
        };
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
            Some(sent) => Completion::native(complete, sent, file, handover_ns, now),
            None => Completion::new(complete, listed, file, handover_ns, now),
        };
        match made {
            Ok(completion) => {
                completing.completion = Some(completion);
                Ok(())
            }
            Err(error) => {
                let status = complete_status(&error);
                self.fail_job(node, at, status)
            }
        }
    }

    /// Asks a completion's driver's request of what serves it; the request back when the node
    /// holds as many asks as it keeps.
    fn ask_complete<N: Serve>(
        &mut self,
        node: &mut N,
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
        // A completion's commands, its list of parts among them, go to their ranges moved, as
        // a put's do (`ask_put`).
        let request = match request {
            complete::Request::File(command) if node.command_room() => {
                return self.ask_command(node, at, id, Layer::File, Command::File(command));
            }
            complete::Request::Name(command) if node.command_room() => {
                return self.ask_command(node, at, id, Layer::Name, Command::Name(command));
            }
            request => request,
        };
        let asked = match &request {
            complete::Request::Upload => node
                .group_of(Layer::Name)
                .ok_or(NodeError::Bounds("no Name range here".into()))
                .and_then(|g| {
                    let query = Query::Upload {
                        bucket,
                        key,
                        upload,
                    };
                    node.ask(JobAsk::Read(g, query))
                }),
            complete::Request::Parts { after, max } => node
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
                    node.ask(JobAsk::Read(g, query))
                }),
            complete::Request::File(_) | complete::Request::Name(_) => Err(NodeError::Asks),
        };
        match asked {
            Ok(pending) => {
                self.wait_on(at, pending, Some(id));
                Ok(None)
            }
            Err(NodeError::Asks) => Ok(Some(request)),
            Err(_) => {
                self.fail_job(node, at, Status::Failed)?;
                Ok(None)
            }
        }
    }

    /// A DeleteObjects' turn (research/89 §5): its bucket's row read first; then each object's
    /// delete asked in the order named, at most an operation's share of the node's commands out
    /// at once (`crate::derive::asks`); once every one is answered, the results, the failures
    /// alone where it is quiet.
    fn step_deletes<N: Serve>(&mut self, node: &mut N, at: usize) -> Result<bool, NodeError> {
        let objects = self.gateway.as_deref().map_or(1, |g| g.objects).max(1);
        // An operation's share of the commands the node holds, past its own enlistment's one.
        let share = node
            .command_asks()
            .saturating_sub(1)
            .checked_div(objects)
            .unwrap_or(1)
            .max(1);
        let Some(Job {
            work: Work::Deletes(deleting),
            pending,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(true);
        };
        let Some((incarnation, versioning)) = deleting.row else {
            if !pending.is_empty() {
                return Ok(false);
            }
            let query = Query::Bucket {
                bucket: deleting.bucket.clone(),
            };
            let Some(group) = node.group_of(Layer::Bucket) else {
                self.fail_job(node, at, Status::Failed)?;
                return self.drain_job(at);
            };
            let asked = node.ask(JobAsk::Read(group, query));
            return self.asked(node, at, asked, None);
        };
        deleting.held = false;
        let Some(group) = node.group_of(Layer::Name) else {
            self.fail_job(node, at, Status::Failed)?;
            return self.drain_job(at);
        };
        let principal = self.jobs.get(at).map(|j| j.principal.clone());
        loop {
            let Some(Job {
                work: Work::Deletes(deleting),
                ..
            }) = self.jobs.get_mut(at)
            else {
                return Ok(true);
            };
            if deleting.out >= share {
                break;
            }
            let index = deleting.next;
            // An object answered already, one its principal may not delete, asks nothing.
            if deleting.results.get(index).is_some_and(Option::is_some) {
                deleting.next = index.saturating_add(1);
                continue;
            }
            let Some(target) = deleting.objects.get(index) else {
                break;
            };
            // A version ID no version is named by is answered here, its delete not asked.
            let named = match named(target.version.as_deref()) {
                Ok(named) => named,
                Err(status) => {
                    let result = Deleted {
                        key: target.key.clone(),
                        version: target.version.clone(),
                        status,
                        marker: None,
                    };
                    if let Some(slot) = deleting.results.get_mut(index) {
                        *slot = Some(result);
                    }
                    deleting.next = index.saturating_add(1);
                    continue;
                }
            };
            // The ETag a delete is conditioned on, as `If-Match` conditions a DeleteObject's
            // (research/89 §5.1).
            let if_match =
                target
                    .etag
                    .as_deref()
                    .map(|etag| match conditional::named(etag, true) {
                        None => Match::Any,
                        Some(tags) => Match::Tags(tags),
                    });
            let command = Command::Name(Box::new(name::Command::Delete(name::Delete {
                bucket: deleting.bucket.clone(),
                incarnation,
                key: target.key.clone(),
                versioning,
                named,
                if_match,
                at_ns: 0,
                bypass: false,
                owner: principal.clone().unwrap_or_default(),
            })));
            match node.ask(JobAsk::Command(group, command)) {
                Ok(asked) => {
                    if let Some(Job {
                        work: Work::Deletes(deleting),
                        ..
                    }) = self.jobs.get_mut(at)
                    {
                        deleting.out = deleting.out.saturating_add(1);
                        deleting.next = index.saturating_add(1);
                        let request = u64::try_from(index).unwrap_or(u64::MAX);
                        self.wait_on(at, asked, Some(request));
                    }
                }
                // The node holds as many commands as it keeps: the rest are asked at a later turn.
                Err(NodeError::Asks) => {
                    if let Some(Job {
                        work: Work::Deletes(deleting),
                        ..
                    }) = self.jobs.get_mut(at)
                    {
                        deleting.held = true;
                    }
                    break;
                }
                Err(_) => {
                    self.fail_job(node, at, Status::Failed)?;
                    return self.drain_job(at);
                }
            }
        }
        let Some(Job {
            work: Work::Deletes(deleting),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(true);
        };
        if deleting.out > 0 || deleting.results.iter().any(Option::is_none) {
            return Ok(false);
        }
        let quiet = deleting.quiet;
        let results = std::mem::take(&mut deleting.results)
            .into_iter()
            .flatten()
            // "In quiet mode the response includes only keys where the delete operation
            // encountered an error" (research/89 §5.2).
            .filter(|d| !quiet || d.status != Status::Done)
            .collect();
        let reply = Reply {
            listing: Some(Listing::Deleted(results)),
            ..Reply::status(Status::Done)
        };
        self.reply_job(node, at, reply)?;
        self.drain_job(at)
    }

    /// A DeleteObjects' bucket row, read: the incarnation and versioning its deletes are made
    /// under, or the reply `NoSuchBucket`.
    fn deletes_read<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        read: Read,
    ) -> Result<(), NodeError> {
        let Read::Bucket(Some(row)) = read else {
            return self.fail_job(node, at, Status::NoSuchBucket);
        };
        if row.state != BucketState::Active {
            return self.fail_job(node, at, Status::NoSuchBucket);
        }
        if let Some(Job {
            work: Work::Deletes(deleting),
            ..
        }) = self.jobs.get_mut(at)
        {
            deleting.row = Some((row.created_ns, row.versioning));
        }
        Ok(())
    }

    /// A copy's turn (research/89 §3–§4): its destination bucket's row read first, an
    /// UploadPartCopy's upload's row next, then the source's version; then its drivers, the
    /// get's plaintext fed to the put as the put takes it, each one's requests asked; once the
    /// put commits, its reply, a null version's time read from its mark first.
    fn step_copy<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        wall: u64,
    ) -> Result<bool, NodeError> {
        let Some(Job {
            work: Work::Copy(copying),
            pending,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(true);
        };
        let read = match copying.stage {
            CopyStage::Bucket => Some((
                Layer::Bucket,
                Query::Bucket {
                    bucket: copying.bucket.clone(),
                },
            )),
            CopyStage::Upload => Some((
                Layer::Name,
                Query::Upload {
                    bucket: copying.bucket.clone(),
                    key: copying.key.clone(),
                    upload: copying
                        .part
                        .as_ref()
                        .map(|(upload, _)| upload.clone())
                        .unwrap_or_default(),
                },
            )),
            CopyStage::Source => {
                let (bucket, key, named) = copying.source.clone();
                Some((
                    Layer::Name,
                    match named {
                        None => Query::Current { bucket, key },
                        Some(named) => Query::Version { bucket, key, named },
                    },
                ))
            }
            CopyStage::Mark => Some((
                Layer::Name,
                Query::Mark {
                    bucket: copying.bucket.clone(),
                    key: copying.key.clone(),
                    id: copying.file,
                },
            )),
            CopyStage::Run => None,
        };
        if let Some((layer, query)) = read {
            if !pending.is_empty() {
                return Ok(false);
            }
            let Some(group) = node.group_of(layer) else {
                self.fail_job(node, at, Status::Failed)?;
                return self.drain_job(at);
            };
            let asked = node.ask(JobAsk::Read(group, query));
            return self.asked(node, at, asked, None);
        }
        // What its put's blocks took to be recorded, for the windows the node's next puts and
        // copies hold (`crate::window`). Its put's body comes at its get's pace, not a client's,
        // so how fast that filled is no client's pace, and is not taken (`Put::take_filled`).
        if let Some(ns) = copying.put.as_mut().and_then(Put::take_committed) {
            node.committed(ns);
        }
        // Blocks its get begins from here are timed from now (`Get::take_read`).
        if let Some(get) = copying.get.as_mut() {
            get.tick(hyper_rt::futures::now_ns());
        }
        // The put's outcome ends the copy: its reply, or its version's time to read first.
        let over = match (
            copying.put.as_ref().and_then(Put::outcome),
            copying.get.as_ref().and_then(Get::outcome),
        ) {
            (Some(Ok(stored)), _) => Some(Ok(stored.clone())),
            // The digests a copy is held to are its source's own, never a client's: bytes that
            // miss them are the store's failure, not the request's `400 BadDigest`.
            (Some(Err(PutError::BadDigest | PutError::BadChecksum)), _) => {
                Some(Err(Reply::status(Status::Failed)))
            }
            (Some(Err(error)), _) => Some(Err(put_failed(error))),
            (None, Some(Err(error))) => Some(Err(get_failed(error))),
            _ => None,
        };
        match over {
            Some(Err(failed)) => {
                self.reply_job(node, at, failed)?;
                return self.drain_job(at);
            }
            Some(Ok(stored)) => {
                let made = stored.version.clone();
                let reply = match (copying.part.is_some(), made.as_deref()) {
                    // A part's row records the time its put committed it.
                    (true, _) => stored
                        .modified_ns
                        .map(|ns| copied(&stored, ns, &copying.source_version)),
                    // A version's order is its time inverted (`mantle_meta::name::put`).
                    (false, Some(id)) if id != NULL_VERSION => parse_version_id(id)
                        .map(|order| copied(&stored, !order, &copying.source_version)),
                    // A null version's ID names no order: its mark does.
                    (false, _) => {
                        copying.stored = Some(stored);
                        copying.stage = CopyStage::Mark;
                        return Ok(false);
                    }
                };
                let reply = reply.unwrap_or_else(|| Reply::status(Status::Failed));
                self.reply_job(node, at, reply)?;
                return self.drain_job(at);
            }
            None => {}
        }
        if let Some(put) = copying.put.as_mut()
            && put.tick(wall).is_err()
            && put.outcome().is_none()
        {
            self.fail_job(node, at, Status::Failed)?;
            return self.drain_job(at);
        }
        self.feed_copy(at);
        // Each driver's requests, each asked of what serves it.
        for driver in [Driver::Get, Driver::Put] {
            loop {
                let Some(job) = self.jobs.get_mut(at) else {
                    return Ok(true);
                };
                let Work::Copy(copying) = &mut job.work else {
                    return self.moved_on(at);
                };
                match driver {
                    Driver::Get => {
                        let next = match copying.get_waiting.pop_front() {
                            Some(request) => Some(request),
                            None => copying.get.as_mut().and_then(Get::poll),
                        };
                        let Some((id, request)) = next else {
                            break;
                        };
                        if let Some(request) = self.ask_get(node, at, id, request)? {
                            if let Some(Job {
                                work: Work::Copy(copying),
                                ..
                            }) = self.jobs.get_mut(at)
                            {
                                copying.get_waiting.push_front((id, request));
                            }
                            break;
                        }
                    }
                    Driver::Put => {
                        let next = match copying.put_waiting.pop_front() {
                            Some(request) => Some(request),
                            None => copying.put.as_mut().and_then(Put::poll),
                        };
                        let Some((id, request)) = next else {
                            break;
                        };
                        if let Some(request) = self.ask_put(node, at, id, request, wall)? {
                            if let Some(Job {
                                work: Work::Copy(copying),
                                ..
                            }) = self.jobs.get_mut(at)
                            {
                                copying.put_waiting.push_front((id, request));
                            }
                            break;
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    /// A copy's plaintext, from its get to its put as far as the put takes it; once the get has
    /// given every byte, the put's body ended, held to the source's own MD5 and checksum where the
    /// copy is of the whole source (research/89 §3.4). A driver that fails keeps its error as its
    /// outcome, which the copy's next turn replies.
    fn feed_copy(&mut self, at: usize) {
        let Some(Job {
            work: Work::Copy(copying),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return;
        };
        let (Some(get), Some(put)) = (copying.get.as_mut(), copying.put.as_mut()) else {
            return;
        };
        loop {
            if copying.carry.is_none() {
                copying.carry = get.take().map(|bytes| (bytes, 0));
            }
            let Some((bytes, took)) = copying.carry.as_mut() else {
                break;
            };
            let rest = bytes.get(*took..).unwrap_or_default();
            if rest.is_empty() {
                copying.carry = None;
                continue;
            }
            if !put.wants_body() {
                return;
            }
            // A feed or an end that fails ends the put with its error, which its outcome holds
            // and the copy's step answers (`Put::guard`).
            match put.feed(rest) {
                Ok(0) | Err(_) => return,
                Ok(n) => *took = took.saturating_add(n),
            }
        }
        if !copying.ended && matches!(get.outcome(), Some(Ok(()))) {
            copying.ended = true;
            let _ = put.end(&copying.expected);
        }
    }

    /// What a copy read: its destination bucket's row, an UploadPartCopy's upload's, its
    /// source's version, or a null version's mark.
    fn copy_read<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        read: Read,
    ) -> Result<(), NodeError> {
        let Some(Job {
            work: Work::Copy(copying),
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        match (copying.stage, read) {
            (CopyStage::Bucket, Read::Bucket(Some(row))) if row.state == BucketState::Active => {
                copying.stage = if copying.part.is_some() {
                    CopyStage::Upload
                } else {
                    CopyStage::Source
                };
                copying.row = Some(row);
                Ok(())
            }
            (CopyStage::Bucket, _) | (CopyStage::Upload, Read::NoBucket) => {
                self.fail_job(node, at, Status::NoSuchBucket)
            }
            (CopyStage::Upload, Read::Upload(Some(upload))) if upload.runs.is_none() => {
                copying.upload = Some(upload);
                copying.stage = CopyStage::Source;
                Ok(())
            }
            // A native upload's parts are its runs, which no copy makes.
            (CopyStage::Upload, Read::Upload(Some(_))) => {
                self.fail_job(node, at, Status::InvalidRequest)
            }
            (CopyStage::Upload, Read::Upload(None)) => {
                self.fail_job(node, at, Status::NoSuchUpload)
            }
            (CopyStage::Source, read) => self.copy_source_read(node, at, read),
            (
                CopyStage::Mark,
                Read::Mark(Some(record::Mark {
                    how: How::Version { order, .. },
                    ..
                })),
            ) => {
                let reply = match &copying.stored {
                    Some(stored) => copied(stored, !order, &copying.source_version),
                    None => Reply::status(Status::Failed),
                };
                self.reply_job(node, at, reply)
            }
            _ => self.fail_job(node, at, Status::Failed),
        }
    }

    /// A copy's source version, read: judged by its copy-source conditions (research/89 §3.3),
    /// what may be copied and how much, then the copy's drivers made, a get of the source's span
    /// and a put of the destination's object or part, its metadata the source's or the request's
    /// by the directive (research/89 §3.2).
    fn copy_source_read<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        read: Read,
    ) -> Result<(), NodeError> {
        let (order, version) = match read {
            Read::NoBucket => return self.fail_job(node, at, Status::NoSuchBucket),
            Read::Current(None) => return self.fail_job(node, at, Status::NoSuchKey),
            // "If the current version is a delete marker, Amazon S3 behaves as if the object was
            // deleted" (research/89 §3.1).
            Read::Current(Some((_, current))) if current.marker => {
                return self.fail_job(node, at, Status::NoSuchKey);
            }
            Read::Current(Some(found)) => found,
            Read::Version(None) => return self.fail_job(node, at, Status::NoSuchVersion),
            Read::Version(Some((_, named))) if named.marker => {
                return self.fail_job(node, at, Status::CopyMarker);
            }
            Read::Version(Some(found)) => found,
            _ => return self.fail_job(node, at, Status::Failed),
        };
        let now = node.wall_ns();
        let Ok(Some(scheme)) = node.table().and_then(Table::scheme) else {
            return self.fail_job(node, at, Status::Failed);
        };
        let Some(gateway) = self.gateway.as_deref() else {
            return self.fail_job(node, at, Status::Failed);
        };
        let Some((generation, root)) = gateway.keys.last() else {
            return self.fail_job(node, at, Status::Failed);
        };
        let keyring = RootKeys {
            keys: gateway
                .keys
                .iter()
                .map(|(g, key)| (*g, *key.bytes()))
                .collect(),
        };
        let made = (|| {
            let data = DataKey::generate().ok()?;
            let wrapped = WrappedKey {
                by: Wrapper::Root(*generation),
                bytes: data.wrap(root).ok()?,
            };
            // The file's ID from the thread's generator, as its blocks' are (`put::id`).
            let keys = Keys {
                file: put::id().ok()?,
                data,
                wrapped,
            };
            Some((keys, Layout::new(scheme).ok()?))
        })();
        let Some((keys, layout)) = made else {
            return self.fail_job(node, at, Status::Failed);
        };
        let Some(Job {
            work: Work::Copy(copying),
            principal,
            charge,
            ..
        }) = self.jobs.get_mut(at)
        else {
            return Ok(());
        };
        let modified = i64::try_from(object::seconds(version.modified_ns)).unwrap_or(i64::MAX);
        let current = Current::Present {
            etag: &version.etag,
            modified,
        };
        match conditional::copy_source(&copying.conditions.conditions(), &current) {
            conditional::Outcome::Proceed => {}
            conditional::Outcome::NotFound => return self.fail_job(node, at, Status::NoSuchKey),
            conditional::Outcome::Failed | conditional::Outcome::NotModified => {
                return self.fail_job(node, at, Status::PreconditionFailed);
            }
        }
        let size = version.size;
        let span = match &copying.part {
            None => {
                // "This copy request is illegal because it is trying to copy an object to itself
                // without changing the object's metadata" (research/89 §3.5).
                let (source_bucket, source_key, named) = &copying.source;
                if *source_bucket == copying.bucket
                    && *source_key == copying.key
                    && named.is_none()
                    && copying.directive == Directive::Copy
                {
                    return self.fail_job(node, at, Status::CopyToItself);
                }
                if size > mantle_s3::body::MAX_UPLOAD {
                    return self.fail_job(node, at, Status::CopySourceTooLarge);
                }
                0..size
            }
            Some(_) => match copying.conditions.range.as_deref() {
                None if size > name::MAX_PART => {
                    return self.fail_job(node, at, Status::CopySourceTooLarge);
                }
                None => 0..size,
                Some(text) => match range::copy_range(text, size) {
                    Err(CopyRangeError::Malformed) => {
                        return self.fail_job(node, at, Status::InvalidCopyRange);
                    }
                    Err(CopyRangeError::Outside) => {
                        return self.fail_job(node, at, Status::InvalidRange);
                    }
                    // "You can copy a range only if the source object is greater than 5 MB"
                    // (research/89 §4.1).
                    Ok(_) if size <= mantle_s3::body::MIN_PART => {
                        return self.fail_job(node, at, Status::CopyRangeSource);
                    }
                    Ok((first, last)) => {
                        let end = last.saturating_add(1);
                        if end.saturating_sub(first) > name::MAX_PART {
                            return self.fail_job(node, at, Status::EntityTooLarge);
                        }
                        first..end
                    }
                },
            },
        };
        let whole = span.start == 0 && span.end == size;
        let length = span.end.saturating_sub(span.start);
        // The checksum the copy keeps: a part's its upload's; an object's the request's, else the
        // source's algorithm, else CRC-64/NVME, as S3 gives a copy a full-object value (research/89
        // §3.4).
        let source_algorithm = version
            .checksum
            .as_ref()
            .and_then(|c| Algorithm::from_code(c.algorithm));
        let algorithm = match (&copying.part, &copying.upload) {
            (Some(_), upload) => upload
                .as_ref()
                .and_then(|u| u.checksum)
                .and_then(|(code, _)| Algorithm::from_code(code)),
            (None, _) => Some(
                copying
                    .checksum
                    .or(source_algorithm)
                    .unwrap_or(Algorithm::Crc64Nvme),
            ),
        };
        // A copy of the whole source is held to the source's own digests: its ETag where that is
        // its bytes' MD5, and its checksum where that is a full-object value of the algorithm
        // the copy keeps.
        copying.expected = Expected {
            md5: if whole {
                md5_of_etag(&version.etag)
            } else {
                None
            },
            checksum: match (&version.checksum, algorithm) {
                (Some(c), Some(algorithm))
                    if whole
                        && c.parts == 0
                        && Algorithm::from_code(c.algorithm) == Some(algorithm) =>
                {
                    Some(checksum::Checksum {
                        algorithm,
                        bytes: c.value.clone(),
                    })
                }
                _ => None,
            },
        };
        let Some(row) = copying.row.as_ref() else {
            return self.fail_job(node, at, Status::Failed);
        };
        let commit = match &copying.part {
            Some((upload, number)) => Commit::Part(name::PutPart {
                bucket: copying.bucket.clone(),
                incarnation: row.created_ns,
                key: copying.key.clone(),
                upload: upload.clone(),
                number: *number,
                part: unfilled_part(),
                at_ns: 0,
                run: None,
            }),
            None => Commit::Object(name::Put {
                bucket: copying.bucket.clone(),
                incarnation: row.created_ns,
                key: copying.key.clone(),
                versioning: row.versioning,
                preconditions: std::mem::take(&mut copying.preconditions),
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
                    headers: match copying.directive {
                        Directive::Copy => version.headers.clone(),
                        Directive::Replace => std::mem::take(&mut copying.metadata),
                    },
                    retention: None,
                    legal_hold: None,
                    listing: None,
                },
                default: row.lock.and_then(|lock| lock.default),
                id: copying.id,
                deadline_ns: 0,
            }),
        };
        let body = Body {
            length,
            checksum: algorithm,
            content_md5: copying.expected.md5.is_some(),
        };
        copying.source_version = name::shown_id(version.null, order);
        copying.file = keys.file;
        // Its windows, now its span's length is known: its put's the one learned, its get's as
        // many blocks as that put drains while one is read, each as wide as the operations' bytes
        // beside the others' hold; the bytes past its admitted charge taken, those its span does
        // not need given back.
        let learned = node.windows();
        let (reads, writes, held) =
            copy_windows(gateway, &learned, |more| node.holds(more), length, *charge);
        node.recharge(held.saturating_sub(*charge), charge.saturating_sub(held));
        *charge = held;
        let holding = Holding {
            handover_ns: gateway.handover_ns,
            window: writes,
        };
        let get = Get::new(version.file, size, span, Box::new(keyring), reads);
        let put = Put::new(commit, body, keys, layout, holding, Box::new(Random), now);
        match (get, put) {
            (Ok(mut get), Ok(put)) => {
                get.tick(hyper_rt::futures::now_ns());
                copying.get = Some(get);
                copying.put = Some(put);
                copying.stage = CopyStage::Run;
                Ok(())
            }
            (_, Err(error)) => self.reply_job(node, at, put_failed(&error)),
            (Err(_), _) => self.fail_job(node, at, Status::Failed),
        }
    }

    /// An operation one read answers, at its turn: its read asked once.
    fn step_single<N: Serve>(&mut self, node: &mut N, at: usize) -> Result<bool, NodeError> {
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
            Single::Uploads(query) | Single::Versions(query) => (Layer::Name, query.clone()),
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
        let Some(group) = node.group_of(layer) else {
            self.fail_job(node, at, Status::Failed)?;
            return self.drain_job(at);
        };
        let asked = node.ask(JobAsk::Read(group, query));
        self.asked(node, at, asked, None)
    }

    /// The answer of an operation one read answers: its reply.
    fn single_read<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        read: Read,
    ) -> Result<(), NodeError> {
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
            (_, Read::Versions(page)) => Reply {
                listing: Some(Listing::Versions(page)),
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
        self.reply_job(node, at, reply)
    }

    /// Ends job `at` with `reply`: what it waits on is let go, and its body, if any is left, read
    /// to its end and dropped before the reply goes.
    fn reply_job<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        reply: Reply,
    ) -> Result<(), NodeError> {
        self.wake(at);
        if let Some(job) = self.jobs.get_mut(at) {
            job.work = Work::Draining(Box::new(reply));
            self.asks
                .release(job.pending.iter().map(|(pending, _)| *pending));
            node.abandon(job.pending.drain(..).map(|(pending, _)| pending));
        }
        #[cfg(test)]
        self.audit();
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
            Some(Work::Answering(_)) => self.answer_door(at),
            Some(_) => Ok(false),
        }
    }

    /// Ends job `at` with `status` alone.
    fn fail_job<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        status: Status,
    ) -> Result<(), NodeError> {
        self.reply_job(node, at, Reply::status(status))
    }

    /// Ends job `at` `SlowDown` for `cause`, which its reply names on either door.
    fn slow_job<N: Serve>(
        &mut self,
        node: &mut N,
        at: usize,
        cause: door::Slowed,
    ) -> Result<(), NodeError> {
        self.reply_job(node, at, Reply::slowed(cause))
    }

    /// A draining job's turn: its reply given to its door, which goes once what is left of its
    /// body is read and dropped; whether it is over: the reply given, or its door gone.
    fn drain_job(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let Work::Draining(reply) =
            std::mem::replace(&mut job.work, Work::Answering(VecDeque::new()))
        else {
            return Ok(false);
        };
        let (bound_ns, admitted) = (job.bound_ns(), job.admitted_ns.is_some());
        match &mut job.door {
            // A put's body the node's task gives from the exchange is let go of, so the node's
            // task reads and drops the rest (`Node::feed_body`); the reply goes after it, the body
            // whole (`Node::write_reply`). A native request answered `SlowDown` is counted for its
            // cause as it goes, as the listener counts the S3 door's. A reply no message holds, a
            // field past its bound, refuses its exchange, so its asker hears it at once.
            Door::Native(_, native) => {
                native.drop_body();
                let told = match reply.encode() {
                    Ok((head, fields)) => Told::Reply(Box::new(Opening {
                        head,
                        fields: Bytes::from(fields),
                        body: 0,
                        slowed: reply.slowed,
                        bound_ns,
                        admitted,
                    })),
                    Err(_) => Told::Failed(Refusal::FrameBound),
                };
                Ok(native.offer(told) != Sent::Held)
            }
            // What is left of the body is the listener's to read or drop (node.md §4.5): the body
            // is let go of, its channel closed, and the reply and its end go now. A connection
            // sends the body before it reads the reply (`listener::conn`'s `stream`), so while it
            // sends it waits on the body's channel until the job takes a piece or lets go; and the
            // reply and its end are two answers, more than a reply's channel of one holds
            // (`door::Replying`: one before a phase is measured, and wherever a phase learns one),
            // so the end waits for the connection to read the reply. Held until the job ended, its
            // end gone, the body kept the connection from the reply and the reply kept the job
            // from its end: the fill suite's S3 put, held for room and then refused
            // `InsufficientStorage` with its body unread, hung until its client gave it up
            // (crates/mantle/tests/serve.rs; CI runs 44575349545, 44578467575 and 44581897076,
            // since 9a312b2b learned the reply's depth from one).
            Door::Local(local) => {
                local.body = None;
                // A reply slowed for a cause says it (`door::Slowed`); any other, its head.
                let head = match reply.slowed {
                    Some(cause) => door::Answer::Slowed(cause),
                    None => door::Answer::Head(reply),
                };
                job.work = Work::Answering(VecDeque::from([head, door::Answer::End]));
                self.answer_door(at)
            }
        }
    }

    /// A job's last answers sent as far as its door's channel takes them; whether it is over:
    /// every one sent, or the door gone.
    fn answer_door(&mut self, at: usize) -> Result<bool, NodeError> {
        let Some(job) = self.jobs.get_mut(at) else {
            return Ok(true);
        };
        let Work::Answering(answers) = &mut job.work else {
            return Ok(true);
        };
        let sent = match &mut job.door {
            Door::Local(local) => {
                local.pending.extend(answers.drain(..));
                local.flush()
            }
            Door::Native(_, native) => native.flush(),
        };
        Ok(sent != Sent::Held)
    }

    /// What job `at`'s client will still wait, in nanoseconds, for an ask of the job's own
    /// (research/77 D2): at least one, as a time that has passed is none to wait on.
    fn remaining_of(&self, at: usize) -> u64 {
        let now = hyper_rt::futures::now_ns();
        self.jobs
            .get(at)
            .map_or(1, |job| job.bound_ns().saturating_sub(now).max(1))
    }
}

/// A put's body from its door's channel, fed to its driver as it wants it: what it did not take
/// of the last piece first, then each piece as it comes, only what it did not take kept, each
/// piece's bytes counted `moved` and its pacing told `took`; and the body's end taken only once
/// the door gave it, its door having checked what the body's framing, signature and checksum
/// say, its trailer's checksum then the one the end is checked against. The status the job ends
/// with where the body does not hold: `IncompleteBody` where its door is gone before its end, its
/// channel closed, or a native door's node's task let go of it (`cut`, `Handed::cut`), read before
/// the channel so a piece given before it is taken first.
fn take_body(
    body: &mut Pieces,
    cut: Option<&std::sync::atomic::AtomicBool>,
    putting: &mut Putting,
    moved: &mut u64,
    mut took: impl FnMut(door::Paced),
) -> Result<(), Status> {
    let Some(put) = putting.put.as_mut() else {
        return Ok(());
    };
    loop {
        if !putting.carry.is_empty() {
            if !put.wants_body() {
                return Ok(());
            }
            let took = put.feed(&putting.carry).map_err(|_| Status::Failed)?;
            putting.taken = putting
                .taken
                .saturating_add(u64::try_from(took).unwrap_or(u64::MAX));
            putting.carry.drain(..took.min(putting.carry.len()));
            if !putting.carry.is_empty() {
                return Ok(());
            }
        }
        if let Some(trailer) = body.ended.take() {
            // The door framed the body at its stated length: an end before or past it is a body
            // that does not hold.
            if putting.taken != putting.length {
                return Err(Status::IncompleteBody);
            }
            if let Some(checksum) = trailer {
                putting.expected.checksum = Some(checksum);
            }
            if put.end(&putting.expected).is_err() && put.outcome().is_none() {
                return Err(Status::Failed);
            }
            // Ended once: nothing more is read, and the outcome answers it.
            putting.length = u64::MAX;
            return Ok(());
        }
        if putting.length == u64::MAX || !put.wants_body() {
            return Ok(());
        }
        let cut = cut.is_some_and(|cut| cut.load(std::sync::atomic::Ordering::Acquire));
        body.dry = false;
        match body.pieces.try_recv() {
            Ok(Some(Piece::Bytes(bytes, paced))) => {
                // The piece's time from its send to this turn: a sample of the body's depth.
                took(paced);
                let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                *moved = moved.saturating_add(len);
                let took = put.feed(&bytes).map_err(|_| Status::Failed)?;
                putting.taken = putting
                    .taken
                    .saturating_add(u64::try_from(took).unwrap_or(u64::MAX));
                if let Some(rest) = bytes.get(took..) {
                    putting.carry.extend_from_slice(rest);
                }
            }
            Ok(Some(Piece::End(trailer))) => body.ended = Some(trailer),
            Ok(Some(Piece::Refused(status))) => return Err(status),
            // The door is gone with the body unfinished.
            Ok(None) if cut => return Err(Status::IncompleteBody),
            Ok(None) => {
                body.dry = true;
                return Ok(());
            }
            Err(_) => return Err(Status::IncompleteBody),
        }
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

/// The work an operation's request names; a native upload's Open cuts its runs in `block`'s whole
/// blocks (`Host::open_block`).
fn work(op: Op, block: Option<u64>) -> Work {
    // Metadata past S3's 2 KB, or that no HTTP field could carry, is refused before anything is
    // written (research/89 §1.4): judged once, its refusal taken in its place below.
    let metadata = match &op {
        Op::Put { metadata, .. }
        | Op::CreateUpload { metadata, .. }
        | Op::Open { metadata, .. } => mantle_s3::metadata::check(metadata).err(),
        Op::Copy(copy) => mantle_s3::metadata::check(&copy.metadata).err(),
        _ => None,
    };
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
        Op::Put { .. } | Op::CreateUpload { .. } | Op::Open { .. } | Op::Copy(_)
            if metadata.is_some() =>
        {
            Work::Draining(Box::new(Reply::status(metadata_status(metadata.as_ref()))))
        }
        Op::Put {
            bucket,
            key,
            id,
            length,
            headers,
            checksum,
            md5,
            trailing,
            metadata,
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
            put: None,
            taken: 0,
            carry: Vec::new(),
            waiting: VecDeque::new(),
            run: None,
            metadata,
        })),
        Op::Get {
            bucket,
            key,
            headers,
            version,
        } => getting(bucket, key, headers, version.as_deref(), false),
        Op::Head {
            bucket,
            key,
            headers,
            version,
        } => getting(bucket, key, headers, version.as_deref(), true),
        Op::Delete {
            bucket,
            key,
            headers,
            version,
            ..
        } => match named(version.as_deref()) {
            Err(status) => Work::Draining(Box::new(Reply::status(status))),
            Ok(named) => Work::Command(Box::new(Commanding {
                bucket,
                then: Then::Delete {
                    key,
                    if_match: matched(headers.if_match.as_deref(), true),
                    named,
                },
                asked: false,
                row: None,
            })),
        },
        Op::PutVersioning {
            bucket, versioning, ..
        } => Work::Command(Box::new(Commanding {
            bucket,
            then: Then::Versioning(match versioning {
                mantle_s3::body::Versioning::Enabled => Versioning::Enabled,
                mantle_s3::body::Versioning::Suspended => Versioning::Suspended,
            }),
            asked: false,
            row: None,
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
            metadata,
            ..
        } => Work::Command(Box::new(Commanding {
            bucket,
            then: Then::CreateUpload {
                key,
                checksum,
                metadata,
            },
            asked: false,
            row: None,
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
            put: None,
            taken: 0,
            carry: Vec::new(),
            waiting: VecDeque::new(),
            run: None,
            metadata: Vec::new(),
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
            row: None,
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
            metadata,
            ..
        } => {
            // No scheme to lay its blocks out by: nothing can be put.
            let Some(block) = block else {
                return Work::Draining(Box::new(Reply::status(Status::Failed)));
            };
            match name::run_length(length, buffer, block) {
                Some(run) => Work::Command(Box::new(Commanding {
                    bucket,
                    then: Then::Open {
                        key,
                        length,
                        checksum,
                        run,
                        metadata,
                    },
                    asked: false,
                    row: None,
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
            }
        }
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
            put: None,
            taken: 0,
            carry: Vec::new(),
            waiting: VecDeque::new(),
            run: Some(Appending {
                offset,
                last,
                incarnation: None,
                runs: None,
            }),
            metadata: Vec::new(),
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
        // No version asked: an empty page, the range not read (research/89 §2.3).
        Op::ListVersions(list) if list.max == 0 => Work::Draining(Box::new(Reply {
            listing: Some(Listing::Versions(object::Versions::default())),
            ..Reply::status(Status::Done)
        })),
        Op::ListVersions(mut list) => {
            list.max = list
                .max
                .min(u32::try_from(mantle_s3::list::MAX_KEYS).unwrap_or(u32::MAX));
            Work::Read(Box::new(Single::Versions(Query::Versions(list))))
        }
        Op::DeleteObjects {
            bucket,
            objects,
            quiet,
            ..
        } => {
            let results = vec![None; objects.len()];
            Work::Deletes(Box::new(Deleting {
                bucket,
                objects,
                quiet,
                row: None,
                next: 0,
                out: 0,
                held: false,
                results,
            }))
        }
        Op::Copy(copy) => {
            let object::Copy {
                bucket,
                key,
                id,
                source,
                conditions,
                headers,
                directive,
                metadata,
                checksum,
            } = *copy;
            let preconditions = Preconditions {
                if_match: matched(headers.if_match.as_deref(), true),
                if_none_match: matched(headers.if_none_match.as_deref(), false),
            };
            copying(
                (bucket, key, None, id),
                source,
                conditions,
                (preconditions, directive, metadata, checksum),
            )
        }
        Op::CopyPart(part) => {
            let object::CopyPart {
                bucket,
                key,
                upload,
                number,
                id,
                source,
                conditions,
            } = *part;
            if !(1..=name::MAX_PART_NUMBER).contains(&number) {
                return Work::Draining(Box::new(Reply::status(Status::InvalidArgument)));
            }
            copying(
                (bucket, key, Some((upload, number)), id),
                source,
                conditions,
                (Preconditions::default(), Directive::Copy, Vec::new(), None),
            )
        }
        Op::CreatePrincipal { name, policy } => administer(|| {
            let compact = checked_policy(&name, &policy)?;
            let id = mantle_s3::account::draw_id().map_err(|_| Status::Failed)?;
            Ok(Admin::Command(Box::new(root::Command::Principal(
                principal::Command::Create(principal::NewPrincipal {
                    name,
                    id,
                    policy: compact,
                    at_ns: 0,
                }),
            ))))
        }),
        Op::PutPolicy { name, policy } => administer(|| {
            let compact = checked_policy(&name, &policy)?;
            Ok(Admin::Command(Box::new(root::Command::Principal(
                principal::Command::Policy {
                    name,
                    policy: compact,
                },
            ))))
        }),
        Op::DeletePrincipal { name } => administer(|| {
            Ok(Admin::Command(Box::new(root::Command::Principal(
                principal::Command::Delete { name },
            ))))
        }),
        // No principal asked: an empty page, nothing read.
        Op::ListPrincipals { max: 0, .. } => Work::Draining(Box::new(Reply {
            listing: Some(Listing::Principals(Principals::default())),
            ..Reply::status(Status::Done)
        })),
        Op::ListPrincipals { after, max } => administer(|| {
            let most = u32::try_from(object::MAX_PAGE_PRINCIPALS).unwrap_or(u32::MAX);
            Ok(Admin::Read {
                query: Query::Principals {
                    after,
                    max: max.min(most),
                },
                one: false,
            })
        }),
        Op::GetPrincipal { name } => administer(|| {
            Ok(Admin::Read {
                query: Query::Principal { name },
                one: true,
            })
        }),
        Op::CreateKey {
            principal,
            rotating,
        } => administer(|| {
            Ok(Admin::Key {
                principal,
                rotating,
            })
        }),
        Op::RevokeKey { id } => administer(|| {
            Ok(Admin::Command(Box::new(root::Command::Principal(
                principal::Command::Revoke { id },
            ))))
        }),
    }
}

/// The refusal `mantle_s3::metadata::check`'s error earns: past S3's 2 KB `MetadataTooLarge`, any
/// other `InvalidArgument`.
fn metadata_status(error: Option<&mantle_s3::metadata::MetadataError>) -> Status {
    match error {
        Some(mantle_s3::metadata::MetadataError::TooLarge) => Status::MetadataTooLarge,
        _ => Status::InvalidArgument,
    }
}

/// A copy's job, its source's version parsed; `InvalidVersion` for one no version is named by.
fn copying(
    (bucket, key, part, id): (String, String, Option<(String, u16)>, u128),
    source: mantle_s3::copy::Source,
    conditions: Headers,
    (preconditions, directive, metadata, checksum): (
        Preconditions,
        Directive,
        Metadata,
        Option<Algorithm>,
    ),
) -> Work {
    let named = match named(source.version.as_deref()) {
        Ok(named) => named,
        Err(status) => return Work::Draining(Box::new(Reply::status(status))),
    };
    Work::Copy(Box::new(Copying {
        bucket,
        key,
        part,
        id,
        source: (source.bucket, source.key, named),
        conditions,
        preconditions,
        directive,
        metadata,
        checksum,
        stage: CopyStage::Bucket,
        row: None,
        upload: None,
        get: None,
        put: None,
        expected: Expected::default(),
        carry: None,
        ended: false,
        get_waiting: VecDeque::new(),
        put_waiting: VecDeque::new(),
        source_version: String::new(),
        file: 0,
        stored: None,
    }))
}

/// An operator's request as work: what it asks, or the reply that refuses it at once.
fn administer(asking: impl FnOnce() -> Result<Admin, Status>) -> Work {
    match asking() {
        Ok(asking) => Work::Admin(Box::new(Administering {
            asking,
            asked: false,
            issued: None,
        })),
        Err(status) => Work::Draining(Box::new(Reply::status(status))),
    }
}

/// A principal's name and identity policy as the root range keeps them: the name in IAM's form,
/// `InvalidArgument` otherwise, and the policy checked as IAM checks a user's, its compact form,
/// `MalformedPolicy` otherwise (`mantle_s3::policy::identity`; research/91 §2, §3).
fn checked_policy(name: &str, policy: &str) -> Result<String, Status> {
    if !mantle_s3::account::valid_name(name) {
        return Err(Status::InvalidArgument);
    }
    mantle_s3::policy::identity(policy.as_bytes())
        .map(|(_, compact)| compact)
        .map_err(|_| Status::MalformedPolicy)
}

fn getting(
    bucket: String,
    key: String,
    headers: Headers,
    version: Option<&str>,
    head: bool,
) -> Work {
    let version = match named(version) {
        Ok(version) => version,
        Err(status) => return Work::Draining(Box::new(Reply::status(status))),
    };
    Work::Get(Box::new(Getting {
        bucket,
        key,
        headers,
        version,
        head,
        get: None,
        held: None,
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

/// The reply a copy earns once its put committed (research/89 §3.6): the new version's or part's
/// ETag, checksum and time, the destination's version ID and the source's.
fn copied(stored: &put::Stored, modified_ns: u64, source_version: &str) -> Reply {
    Reply {
        version: stored.version.clone().unwrap_or_default(),
        etag: stored.etag.clone(),
        size: stored.size,
        modified: object::seconds(modified_ns),
        checksum: stored.checksum.as_ref().map(kept),
        source_version: source_version.to_owned(),
        ..Reply::status(Status::Done)
    }
}

/// The MD5 an ETag is, where it is one: 32 hexadecimal digits, a single PUT's of plaintext
/// under SSE-S3 (research/05 §5.1); a multipart object's has its parts' count after it.
fn md5_of_etag(etag: &str) -> Option<[u8; 16]> {
    let digits = etag.as_bytes();
    if digits.len() != 32 {
        return None;
    }
    let mut md5 = [0u8; 16];
    for (byte, pair) in md5.iter_mut().zip(digits.as_chunks::<2>().0) {
        let hex = std::str::from_utf8(pair).ok()?;
        *byte = u8::from_str_radix(hex, 16).ok()?;
    }
    Some(md5)
}

/// The reply a get's failure is answered with: too few of a block's chunks read, or no room for
/// its reads within its client's patience, is a cause that passes, `503 SlowDown`, which clients
/// ask again; any other `500`.
fn get_failed(error: &GetError) -> Reply {
    match error {
        GetError::Unreadable(_) => Reply::slowed(door::Slowed::Unreadable),
        GetError::Patience => Reply::slowed(door::Slowed::Patience),
        _ => Reply::status(Status::Failed),
    }
}

/// What a job's ask came to.
pub(crate) enum Outcome {
    Command(CommandOutcome),
    Read(ReadOutcome),
    Chunk(ChunkOutcome),
}

/// A put's driver's answer for what its request came to.
fn put_answer(outcome: Outcome) -> put::Answer {
    match outcome {
        Outcome::Chunk(chunk) => match taken(&chunk) {
            Taken::Stored => put::Answer::Stored,
            Taken::Room => put::Answer::Busy,
            Taken::Refused => put::Answer::Refused { full: false },
            Taken::Full => put::Answer::Refused { full: true },
            Taken::Failed => put::Answer::Failed,
        },
        Outcome::Command(CommandOutcome::Answered(Answer::Block(o))) => put::Answer::Block(o),
        Outcome::Command(CommandOutcome::Answered(Answer::File(o))) => put::Answer::File(o),
        Outcome::Command(CommandOutcome::Answered(Answer::Name(o))) => put::Answer::Name(o),
        // Any other: the driver takes it as the wrong kind and fails the put.
        _ => put::Answer::Failed,
    }
}

/// Where a chunk's put goes next, by what its outcome means ([`taken`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Taken {
    /// Durable on its volume.
    Stored,
    /// No room for it now, at its volume or in the transport toward it: the volume takes it once
    /// room frees, so it is asked of the same volume again then, within its client's patience
    /// (research/77 D2), and its refusal is no movement of its work.
    Room,
    /// This volume does not take it, and another may: the next volume offered is asked.
    Refused,
    /// The volume has no room, and cleaning can make none.
    Full,
    /// No volume takes it as it was sent: the put cannot go on.
    Failed,
}

/// What a chunk put's outcome means for where the chunk goes next: each of the volume's statuses
/// (`chunk::Status`) and each of the transport's refusals (`Refusal`, as
/// vendor/hyper-transport/src/error.rs defines them) is classed in one arm of its class, with no
/// catch-all, so a status or a refusal added does not compile until it is classed.
///
/// A put asked again of a volume that may already hold it is safe: a chunk's key is its block's id,
/// its epoch and its index (`put::chunk_key`), and the volume's writer answers a whole chunk's put
/// (`Volume::put_checked_waking`: offset 0, sealed) under a key it holds, with the same length,
/// CRC-32C and bytes, as done without writing it again, and refuses other bytes under it `Exists`
/// (`mantle_chunk`'s writer, `validate`; audit B05; node.md §5.2). A chunk read's refusals are
/// classed by it too (`get_answer`): a read asked again changes nothing, and its room, its place
/// and its sending mean for it what they mean for a put.
pub(super) fn taken(outcome: &ChunkOutcome) -> Taken {
    match outcome {
        ChunkOutcome::Stored => Taken::Stored,
        // A get its peer's suspicion kept from it: this place gives nothing now, another may.
        ChunkOutcome::Suspected => Taken::Refused,
        // A get's bytes answering a put: the node crossed its asks, which no volume mends.
        ChunkOutcome::Read(_) => Taken::Failed,
        ChunkOutcome::Refused(status) => match status {
            // A put's own word, wrapped as a refusal only for a get (`Node::chunk_replied`): the
            // node crossed its asks, and nothing says the chunk is durable.
            chunk::Status::Stored => Taken::Failed,
            // The volume's writer queue holds all it takes: a write ending makes room.
            chunk::Status::Busy => Taken::Room,
            chunk::Status::Full => Taken::Full,
            // This volume, not the chunk: fenced after a device error, closing, not held by the
            // node asked, the bytes changed on the way to it, past what its records hold, its own
            // records damaged, other bytes under the key, its head or its reply undecoded, failed
            // otherwise, or a get's status answering a put.
            chunk::Status::Fenced
            | chunk::Status::Closed
            | chunk::Status::NoVolume
            | chunk::Status::Checksum
            | chunk::Status::TooLarge
            | chunk::Status::Corrupt
            | chunk::Status::Exists
            | chunk::Status::Malformed
            | chunk::Status::Failed
            | chunk::Status::Read
            | chunk::Status::NotFound
            | chunk::Status::Range => Taken::Refused,
        },
        ChunkOutcome::Lost(refusal) => match refusal {
            // A bound reached for now, here or at the peer, none of it the volume's: the budget
            // cannot fund the bytes; the exchange table, a lane's queue, the lanes, the owner's
            // unpolled events or the socket's receive queue at their bound; the handshakes, the
            // identities or the connections at theirs, or the identity's every connection busy,
            // while a connection is made (a peer full, not gone: `Event::Unreachable`). `Unsent`:
            // it waited a period for a stream its peer did not grant, nothing sent, as a peer
            // granting streams as wanted holds requests back while it is busy. `Stalled`: a period
            // moved less than a datagram, its peer too busy to take it, and what reached the
            // volume is answered as done when asked again (above).
            Refusal::Budget
            | Refusal::Exchanges
            | Refusal::LaneFull
            | Refusal::Lanes
            | Refusal::Events
            | Refusal::ReceiveBuffer
            | Refusal::Pending
            | Refusal::Identities
            | Refusal::Connections
            | Refusal::Busy
            | Refusal::Unsent
            | Refusal::Stalled => Taken::Room,
            // This peer, not the chunk: not the one the directory names, no connection up to it,
            // its connection closed or the exchange ended unanswered, the bytes damaged on the way,
            // its connection's QUIC refused, or a refusal this side does not know.
            Refusal::Identity
            | Refusal::NotConnected
            | Refusal::Closed
            | Refusal::UnknownExchange
            | Refusal::Corrupt
            | Refusal::Quic
            | Refusal::Unknown => Taken::Refused,
            // Of this node or the message, alike at every volume: its role may not send a chunk's
            // put, the message is past its class's frame bound, the node broke the exchange's
            // order, its exchange identifiers ran out, or the exchange's settings are invalid.
            Refusal::Kind
            | Refusal::FrameBound
            | Refusal::Order
            | Refusal::Exhausted
            | Refusal::Configuration => Taken::Failed,
        },
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
        // A put's word from a volume answering a read (`Node::chunk_replied`): that place answers
        // no read, and another may.
        Outcome::Chunk(ChunkOutcome::Refused(chunk::Status::Stored)) => get::Answer::Unreadable,
        // Its peer suspected: another place is read first, and this one past the suspicion only
        // where the block cannot be read without it.
        Outcome::Chunk(ChunkOutcome::Suspected) => get::Answer::Suspected,
        // The volume holds no such chunk, or the node asked no such volume: the driver's row of
        // the block may predate a move of its chunk (repair.md §3–4), so another place is read,
        // or the row read again where none gives the block. A chunk that fails its checksum is
        // `Corrupt`, no move: the volume reports it for repair, and it is unreadable below.
        Outcome::Chunk(ChunkOutcome::Refused(
            chunk::Status::NotFound | chunk::Status::NoVolume,
        )) => get::Answer::Absent,
        // Every other outcome by its class ([`taken`]), as a put's is.
        Outcome::Chunk(chunk) => match taken(&chunk) {
            // No room for the read now, at the volume's reads or in the transport toward it, or
            // its peer too busy to move it a period: the place still holds the bytes, is asked
            // again once room frees within its client's patience (research/77 D2), and no copy
            // is taken for lost.
            Taken::Room => get::Answer::Busy,
            // This place cannot give them: its volume fenced, closing or not on the node asked,
            // the chunk not found there, past its end or corrupt, its head or a put's status
            // answering, or its peer gone or not the one named. Another place that holds them
            // is read, or the block decoded.
            Taken::Refused | Taken::Full => get::Answer::Unreadable,
            // No volume serves it as it was asked (the node's role, its frame bound, the
            // exchange's order, identifiers or settings), or a put's outcome answering a get:
            // the node crossed its asks.
            Taken::Failed | Taken::Stored => get::Answer::Failed,
        },
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
pub(super) mod tests {
    use super::*;

    std::thread_local! {
        /// The shard's clock's reads this thread made through [`clock`].
        pub(super) static CLOCK_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
        /// The jobs this thread's hosts stepped ([`Host::step_job`]).
        pub(super) static STEPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
        /// The wall clock's reads this thread's stubs of the node gave ([`Stub`]'s `wall_ns`).
        pub(super) static WALL_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
        /// The probes this thread made of a host's slots ([`Slots::get`]).
        pub(super) static PROBES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        /// The keys this thread's hosts compared in their heaps ([`Earliest::less`]).
        pub(super) static COMPARED_KEYS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        /// The jobs this thread's hosts' turns read: each bounded ([`Host::bound_job`]), offered
        /// room ([`Host::offer_room`]) or taken to be stepped ([`Host::step`]).
        pub(super) static READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }

    /// The jobs this thread's hosts' turns read since this was last asked.
    fn reads() -> u64 {
        READS.with(|reads| reads.replace(0))
    }

    impl Host {
        /// The oracle, after each step of a host and each change to its jobs: its slots agree with
        /// a full recount, each exchange and each ask its jobs hold at the slot of the job holding
        /// it, and no other.
        pub(super) fn audit(&self) {
            let mut exchanges = 0;
            let mut asks = 0;
            for (at, job) in self.jobs.iter().enumerate() {
                if let Some(exchange) = job.door.exchange() {
                    exchanges += 1;
                    assert_eq!(self.exchanges.of.get(&exchange), Some(&at), "{exchange:?}");
                }
                for (pending, _) in &job.pending {
                    asks += 1;
                    assert_eq!(self.asks.of.get(pending), Some(&at), "{pending:?}");
                }
            }
            assert_eq!(
                self.exchanges.len(),
                exchanges,
                "an exchange's slot no job holds"
            );
            assert_eq!(self.asks.len(), asks, "an ask's slot no job waits on");
            self.beats.audit(self.jobs.len(), "beat");
            self.renewals.audit(self.jobs.len(), "renewal");
            self.rooms.audit(self.jobs.len(), "room");
            self.ready.audit(self.jobs.len(), "wake");
            self.handovers.audit(self.jobs.len(), "handover");
            for (at, job) in self.jobs.iter().enumerate() {
                assert_eq!(
                    self.ready.key(at).is_some(),
                    !job.resting,
                    "job {at} awake exactly where it does not rest"
                );
            }
        }

        /// Job `at` rests, as its turn found nothing to do: a test's job made so at once.
        pub(super) fn lull(&mut self, at: usize) {
            self.jobs[at].resting = true;
            self.ready.set(at, None);
        }

        /// The oracle as a step ends at `now`, every job turned in it: [`Host::audit`], and each
        /// job's keys what its state makes them at `now` (`Host::rekey`), the earliest of each
        /// what a full recount finds.
        ///
        /// A job a step did not read, resting with nothing due, keeps the keys its last turn set:
        /// each is never later than its state makes it at `now`, and set exactly where its state
        /// sets one. A job waits among the waiters for room exactly where its chunk waits for it.
        pub(super) fn audit_keys(&self, now: u64) {
            self.audit();
            let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap();
            let mut beats = Vec::new();
            for (at, job) in self.jobs.iter().enumerate() {
                let due = job.beat_ns(now, beat);
                let key = self.beats.key(at);
                assert_eq!(
                    key.is_some(),
                    due.is_some(),
                    "job {at}'s beat {key:?}, {due:?}"
                );
                assert!(key <= due, "job {at}'s beat {key:?} past {due:?}");
                assert_eq!(
                    self.renewals.key(at),
                    job.renewal_ns(),
                    "job {at}'s renewal"
                );
                assert_eq!(
                    self.rooms.key(at).is_some(),
                    job.waits_for_room(),
                    "job {at} among the waiters for room"
                );
                assert_eq!(
                    self.handovers.key(at).is_some(),
                    matches!(job.work, Work::Handing(_)),
                    "job {at} among the handovers exactly where it is handed"
                );
                beats.extend(key);
            }
            assert_eq!(self.beats.first(), beats.into_iter().min());
            let renewal = self.jobs.iter().filter_map(Job::renewal_ns).min();
            assert_eq!(self.renewals.first(), renewal);
        }
    }

    impl Earliest {
        /// The oracle's recount of the keys of `slots` slots, each an `of`: each slot's place
        /// naming its entry, no key less than its parent's, and an entry a slot keyed.
        fn audit(&self, slots: usize, of: &str) {
            assert_eq!(self.place.len(), slots, "a {of}'s place a slot");
            for (place, (key, at)) in self.heap.iter().enumerate() {
                assert_eq!(self.place[*at], Some(place), "slot {at}'s {of}'s place");
                if let Some(parent) = place.checked_sub(1).map(|p| p / 2) {
                    assert!(self.heap[parent].0 <= *key, "{of}s out of order at {place}");
                }
            }
            assert_eq!(
                self.place.iter().flatten().count(),
                self.heap.len(),
                "a {of} a slot keyed"
            );
        }
    }

    impl Replies {
        /// Whether, by its own state and its job's alone, its visit would move it now: waiting for
        /// no body, its job said its work is held, or, writing nothing, its job gave an answer or
        /// ended. What its exchange says (`BodyReady`, `Writable`) marks it as it is said
        /// (`Node::on_event`), and one waiting for room in the endpoint's budget is marked as the
        /// budget gives bytes back or its next vouch comes (`Writes::wake_rooms`): nothing of its
        /// own moves it.
        pub(super) fn could_move(&self) -> bool {
            let load = |word: &std::sync::atomic::AtomicBool| {
                word.load(std::sync::atomic::Ordering::Acquire)
            };
            let told = self.handed.told.load(std::sync::atomic::Ordering::Acquire) > 0;
            self.drained.is_none()
                && (load(&self.handed.held)
                    || (self.out.is_none() && (told || load(&self.handed.ended))))
        }
    }

    impl Feeding {
        /// Whether, by its own state and its job's alone, its visit would move it now: its job let
        /// go of a body it still reads for it, or took a piece since it last looked, which frees
        /// the channel's room and the storage the next is read into. What its exchange says
        /// (`BodyReady`) marks it as it is said (`Node::on_event`).
        pub(super) fn could_move(&self) -> bool {
            (self.job_let_go() && !self.dropping)
                || self.handed.took.load(std::sync::atomic::Ordering::Acquire) > 0
        }
    }

    impl<K: Copy> Feedings<K>
    where
        (K, Feeding): Key<Of = K>,
    {
        /// The oracle, after each change: its list's (`Readied::audit`), and a recount of every
        /// feeding: each that could move now marked, taken to visit, visited, or named by its job
        /// on the node's marks (`Handed::marked`).
        pub(super) fn audit(&self) {
            self.feeding.audit();
            for (exchange, feeding) in self.feeding.iter() {
                if feeding.could_move() {
                    assert!(
                        self.feeding.covered(*exchange)
                            || feeding
                                .handed
                                .marked
                                .load(std::sync::atomic::Ordering::Acquire),
                        "a feeding that could move, unmarked"
                    );
                }
            }
        }
    }

    impl<K: Copy> Writes<K>
    where
        (K, Replies): Key<Of = K>,
    {
        /// The oracle, after each change: its slots exact (`Keyed::audit`), its heap's places and
        /// order ([`Earliest::audit`]), each reply's key its due, and the first the earliest a
        /// full recount finds; and a recount of every reply: each that could move now marked,
        /// taken to visit, visited, or named by its job on the node's marks (`Handed::marked`).
        pub(super) fn audit(&self) {
            self.replies.audit();
            for (exchange, replies) in self.replies.iter() {
                if replies.could_move() {
                    assert!(
                        self.replies.covered(*exchange)
                            || replies
                                .handed
                                .marked
                                .load(std::sync::atomic::Ordering::Acquire),
                        "a reply that could move, unmarked"
                    );
                }
            }
            self.due.audit(self.replies.len(), "due");
            let beat = room_beat();
            for (at, (_, replies)) in self.replies.iter().enumerate() {
                assert_eq!(
                    self.due.key(at),
                    replies.room_due_ns(beat),
                    "reply {at}'s due"
                );
            }
            let earliest = self
                .replies
                .iter()
                .filter_map(|(_, replies)| replies.room_due_ns(beat))
                .min();
            assert_eq!(self.due.first(), earliest);
        }
    }

    /// The clock's reads and the jobs stepped on this thread since the last call.
    pub(super) fn clock_reads() -> (u64, u64) {
        (
            CLOCK_READS.with(|r| r.replace(0)),
            STEPS.with(|s| s.replace(0)),
        )
    }

    /// A turn of a host holding 64 puts waiting on their bodies, of which one's body ends, reads
    /// the clock as the turn begins and once after the one step that did work, its commit asked:
    /// none around the 63 steps that found nothing (`Timing`). Before, a turn read it as it began,
    /// as it bounded its jobs, and twice around each step: 2 + 2 × 64 = 130.
    #[test]
    fn a_turn_reads_the_clock_once_and_once_more_for_each_step_that_did_work() {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(64, Some(std::sync::Arc::new(scheming_gateway())));
        let mut bodies = Vec::new();
        let mut replies = Vec::new();
        for _ in 0..64 {
            let mut putting = native_put(0);
            putting.trailing = None;
            let (job, body, answers) = local_job(Work::Put(putting), now, 7);
            host.push(job, now);
            bodies.push(body);
            replies.push(answers);
        }
        let mut stub = put_and_get_stub();
        host.step(&mut stub).unwrap();
        assert_eq!(stub.asked, 0, "no body came: nothing is asked");
        let _ = clock_reads();
        let crc = checksum::checksum(Algorithm::Crc32c, &[]).unwrap();
        assert!(bodies[0].try_send(Piece::End(Some(crc))).is_ok());
        host.step(&mut stub).unwrap();
        assert_eq!(
            stub.asked, 1,
            "the one put whose body ended asks its commit"
        );
        let (reads, steps) = clock_reads();
        assert_eq!(steps, 64, "every job is still visited");
        assert_eq!(reads, 2, "the turn's read and the one working step's");
    }

    /// Steps a put: 64 puts of an empty body, each committed by one Name command answered 32 turns
    /// after it is asked, as a range's log answers a commit many turns of a loaded node's task
    /// later. Each put is stepped as its turn first comes, its body's end taken and its commit
    /// asked, and again as its answer comes, its reply given and its job let go: the 31 turns
    /// between, where it waits on its commit alone, pass it by (`Job::rests`), so each put is
    /// stepped twice, 128 steps, where it was stepped at every turn, 2,112 steps, 33 a put; and
    /// each turn reads the wall clock once where each step of a put read it, 2,112 reads.
    #[test]
    fn a_put_waiting_on_its_asks_alone_is_stepped_only_as_they_are_answered() {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(64, Some(std::sync::Arc::new(scheming_gateway())));
        let crc = checksum::checksum(Algorithm::Crc32c, &[]).unwrap();
        let mut bodies = Vec::new();
        let mut replies = Vec::new();
        for _ in 0..64 {
            let mut putting = native_put(0);
            putting.trailing = None;
            let (job, body, answers) = local_job(Work::Put(putting), now, 7);
            assert!(body.try_send(Piece::End(Some(crc.clone()))).is_ok());
            host.push(job, now);
            bodies.push(body);
            replies.push(answers);
        }
        let mut stub = put_and_get_stub();
        let mut held = Vec::new();
        let _ = clock_reads();
        WALL_READS.with(|reads| reads.set(0));
        let mut turns = 0;
        while !host.idle() {
            turns += 1;
            assert!(turns <= 64, "the puts never ended");
            if turns == 33 {
                stub.answers.append(&mut held);
            }
            host.step(&mut stub).unwrap();
            held.append(&mut stub.answers);
        }
        let (_, steps) = clock_reads();
        let walls = WALL_READS.with(|reads| reads.replace(0));
        assert_eq!(turns, 33, "asked at the first turn, answered at the 33rd");
        assert_eq!(stub.asked, 64, "each put's one commit");
        assert_eq!(
            steps,
            2 * 64,
            "each put stepped as it begins and as it is answered"
        );
        assert_eq!(walls, 33, "the wall clock read once a turn");
        for answers in &mut replies {
            let answers = answered(answers);
            assert!(
                matches!(answers.as_slice(), [door::Answer::Ready, door::Answer::Head(reply), door::Answer::End]
                    if reply.status == Status::Done && reply.version == "v"),
                "{answers:?}"
            );
        }
    }

    /// A turn reads the jobs its events name, not the jobs held: 1,024 puts of an empty body, each
    /// committed by one Name command answered 32 turns after it is asked. Each put is read as its
    /// turn first comes and as its answer comes, 2,048 reads in all, and the 31 turns between read
    /// none, where the walk before read every put three times a turn: whether any waited for room,
    /// its bound and vouch, and its resting flag, 3 × 1,024 × 33 = 101,376 reads. Each answered
    /// `Done` with its version.
    #[test]
    fn a_turn_reads_the_jobs_its_events_name_not_every_job_held() {
        const PUTS: usize = 1_024;
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(PUTS, Some(std::sync::Arc::new(scheming_gateway())));
        let crc = checksum::checksum(Algorithm::Crc32c, &[]).unwrap();
        let mut bodies = Vec::new();
        let mut replies = Vec::new();
        for _ in 0..PUTS {
            let mut putting = native_put(0);
            putting.trailing = None;
            let (job, body, answers) = local_job(Work::Put(putting), now, 7);
            assert!(body.try_send(Piece::End(Some(crc.clone()))).is_ok());
            host.push(job, now);
            bodies.push(body);
            replies.push(answers);
        }
        let mut stub = put_and_get_stub();
        let mut held = Vec::new();
        let _ = reads();
        let mut read = Vec::new();
        let mut turns = 0;
        while !host.idle() {
            turns += 1;
            assert!(turns <= 64, "the puts never ended");
            if turns == 33 {
                stub.answers.append(&mut held);
            }
            host.step(&mut stub).unwrap();
            read.push(reads());
            held.append(&mut stub.answers);
        }
        assert_eq!(turns, 33, "asked at the first turn, answered at the 33rd");
        assert_eq!(stub.asked, PUTS as u64, "each put's one commit");
        assert_eq!(
            read[0], PUTS as u64,
            "each put read as its turn first comes"
        );
        assert!(
            read[1..32].iter().all(|&read| read == 0),
            "the turns between read none: {read:?}"
        );
        assert_eq!(read[32], PUTS as u64, "each put read as its answer comes");
        for answers in &mut replies {
            let answers = answered(answers);
            assert!(
                matches!(answers.as_slice(), [door::Answer::Ready, door::Answer::Head(reply), door::Answer::End]
                    if reply.status == Status::Done && reply.version == "v"),
                "{answers:?}"
            );
        }
    }

    /// The bound pass reads the jobs due alone, and tells each asker its work is held or moves at
    /// the turn the walk of every job at every turn did: 64 native puts, half held on an ask of
    /// their own and half at work holding nothing, each one's work moving every second to eighth
    /// turn, turned 64 times a quarter of a vouch's interval apart. Run beside the walk on a twin
    /// host, each job's last vouch is the walk's at every turn, 1,110 vouches each; the pass read
    /// the 1,110 jobs it told, and no other, where the walk read 4,096.
    #[test]
    fn the_bound_pass_reads_the_jobs_due_and_vouches_when_the_walk_did() {
        const JOBS: usize = 64;
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap();
        let quarter = vouch_every(beat) / 4;
        let start = 1_000;
        let mut stub = put_and_get_stub();
        let mut hosts = [Host::new(JOBS, None), Host::new(JOBS, None)];
        let mut doors = Vec::new();
        for host in &mut hosts {
            for at in 0..JOBS {
                let (job, replies) = handed_job(Work::Put(native_put(1)), start, 0);
                host.push(job, start);
                doors.push(replies);
                if at % 2 == 0 {
                    host.wait_on(at, Pending::Chunk(at as u64), None);
                    host.rekey(at, start);
                }
            }
        }
        let (mut read, mut walked, mut vouches) = (0, 0, 0);
        for turn in 1..=64u64 {
            let now = start + turn * quarter;
            for host in &mut hosts {
                for at in 0..JOBS {
                    // Its work moves between turns, as a piece of its chunk does (`chunk_moved`).
                    if turn % (at as u64 % 7 + 2) == 0 {
                        host.jobs[at].moved_ns = now - 1;
                        host.rekey(at, now - 1);
                    }
                }
            }
            let before: Vec<_> = hosts[0].jobs.iter().map(|job| job.vouched_ns).collect();
            let _ = reads();
            hosts[0].bound_jobs(&mut stub, now, beat).unwrap();
            read += reads();
            hosts[0].audit_keys(now);
            // The rule before: every job bounded at every turn, and keyed again.
            for at in 0..JOBS {
                hosts[1].bound_job(&mut stub, at, now, beat).unwrap();
                hosts[1].rekey(at, now);
            }
            walked += reads();
            let pass: Vec<_> = hosts[0].jobs.iter().map(|job| job.vouched_ns).collect();
            let walk: Vec<_> = hosts[1].jobs.iter().map(|job| job.vouched_ns).collect();
            assert_eq!(pass, walk, "turn {turn}: each job's last vouch");
            vouches += pass
                .iter()
                .zip(&before)
                .filter(|(pass, before)| pass != before)
                .count();
        }
        assert_eq!(walked, 64 * JOBS as u64);
        assert_eq!((read, vouches), (1_110, 1_110), "jobs read, vouches told");
    }

    /// The S3 connection of a job made through a door, as the listener holds it: its submitter,
    /// which names the request it submitted last, the body's sender, the reply's receiver, and its
    /// hold on the request (`door::Named`).
    struct Connection {
        submitter: door::Submitter,
        body: Sender<Piece>,
        answers: ChannelReceiver<door::Answer>,
        _named: door::Named,
    }

    /// A job of `work` admitted at `now` and charged `charge`, its request `op` submitted at
    /// `doorway` by a connection of its own with a reply's channel of `room` answers, as the
    /// listener submits one (`listener::conn`): its door names it by the request's mark.
    fn named_job(
        (work, op): (Work, Op),
        now: u64,
        charge: u64,
        (submitter, doorway): (&door::Submitter, &mut door::Doorway),
        room: usize,
    ) -> (Job, Connection) {
        let (body, pieces) = hyper_rt::sync::channel(2).unwrap();
        let (reply, answers) = hyper_rt::sync::channel(room).unwrap();
        let mut submitter = submitter.clone();
        let named = submitter
            .submit(door::Request {
                op,
                principal: "o".into(),
                denied: Vec::new(),
                body: pieces,
                reply,
                mark: None,
            })
            .unwrap();
        let Some(door::Asked::Op(request)) = doorway.take() else {
            panic!("the request submitted");
        };
        let door::Request {
            body: pieces,
            reply,
            mark,
            ..
        } = *request;
        assert!(mark.is_some(), "named as it was submitted");
        let (mut job, _, _) = local_job(work, now, charge);
        job.door = Door::Local(Box::new(Local {
            body: Some(Pieces {
                pieces,
                ended: None,
                dry: false,
            }),
            reply,
            pending: VecDeque::new(),
            ready: false,
            mark,
        }));
        let connection = Connection {
            submitter,
            body,
            answers,
            _named: named,
        };
        (job, connection)
    }

    /// A get of `b/k`'s current version.
    fn get_op() -> Op {
        Op::Get {
            bucket: "b".into(),
            key: "k".into(),
            headers: Headers::default(),
            version: None,
        }
    }

    /// The steps each of `turns` turns of `host` took, the door's names taken before each.
    fn stepped(
        host: &mut Host,
        stub: &mut Stub,
        doorway: &mut door::Doorway,
        turns: usize,
    ) -> Vec<u64> {
        (0..turns)
            .map(|_| {
                host.take_marks(doorway);
                let _ = clock_reads();
                host.step(stub).unwrap();
                clock_reads().1
            })
            .collect()
    }

    /// Steps GETs of an empty object through the S3 door whose connections have no room for their
    /// replies: 64 gets, each reply's channel holding one answer and its connection reading none.
    /// Each get is stepped as it asks its version and as the version comes, its head given and its
    /// end held for want of room; then, its connection reading nothing, 32 turns pass every get by
    /// (`Job::rests`), where every turn stepped every get; and as each connection takes the head
    /// and names its request (`door::Submitter::wake`), its get alone is stepped, giving its end
    /// and ending. 192 steps over 36 turns, three a get, where every turn stepped every get, 2,272,
    /// 35 or 36 a get; and each get answered its head and end, no wake lost.
    #[test]
    fn an_s3_get_with_no_room_for_its_reply_is_stepped_only_as_its_connection_names_it() {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(64, Some(std::sync::Arc::new(scheming_gateway())));
        let (mut doorway, submitter) = door::doorway(64, std::task::Waker::noop().clone()).unwrap();
        let read = scheming_gateway().charges.read(1);
        let mut connections = Vec::new();
        for _ in 0..64 {
            let get = (work(get_op(), None), get_op());
            let (job, connection) = named_job(get, now, read, (&submitter, &mut doorway), 1);
            host.push(job, now);
            connections.push(connection);
        }
        let mut stub = put_and_get_stub();
        // Its version asked, then read: its head given, its end held for want of room.
        assert_eq!(stepped(&mut host, &mut stub, &mut doorway, 2), [64, 64]);
        // Its connection reads nothing: no get is stepped.
        let idle = stepped(&mut host, &mut stub, &mut doorway, 32);
        assert_eq!(idle.iter().sum::<u64>(), 0, "{idle:?}");
        // Half the connections take the head and name their gets: those alone are stepped.
        let mut heads = Vec::new();
        for connection in connections.iter_mut().take(32) {
            heads.push(connection.answers.try_recv().unwrap());
            connection.submitter.wake();
        }
        assert_eq!(stepped(&mut host, &mut stub, &mut doorway, 1), [32]);
        for connection in connections.iter_mut().skip(32) {
            heads.push(connection.answers.try_recv().unwrap());
            connection.submitter.wake();
        }
        assert_eq!(stepped(&mut host, &mut stub, &mut doorway, 1), [32]);
        assert!(host.idle(), "every get ended");
        assert_eq!(stub.asked, 64, "each get's one read");
        for (head, connection) in heads.iter().zip(&mut connections) {
            assert!(
                matches!(head, Some(door::Answer::Head(reply)) if reply.status == Status::Done && reply.size == 0),
                "{head:?}"
            );
            let rest = answered(&mut connection.answers);
            assert_eq!(rest, [door::Answer::End]);
        }
    }

    /// Steps puts of an empty body through the S3 door whose connections send their bodies late:
    /// 64 puts, each told its driver wants its body (`door::Answer::Ready`) at its first turn and
    /// finding its body's channel empty; 32 turns then pass every put by (`Job::rests`), where
    /// every turn stepped every put waiting on its body; and as each connection sends its body's
    /// end and names its request, each put alone is stepped, taking its end and asking its commit,
    /// and again as its commit is answered. 192 steps over 35 turns, three a put, where every turn
    /// stepped every put waiting on its body, 2,240, 35 a put; and each put answered `Done` with
    /// its version, no wake lost.
    #[test]
    fn an_s3_put_waiting_on_its_body_is_stepped_only_as_its_connection_names_it() {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(64, Some(std::sync::Arc::new(scheming_gateway())));
        let (mut doorway, submitter) = door::doorway(64, std::task::Waker::noop().clone()).unwrap();
        let mut connections = Vec::new();
        for _ in 0..64 {
            let mut putting = native_put(0);
            putting.trailing = None;
            let op = Op::Put {
                bucket: "b".into(),
                key: "k".into(),
                id: 1,
                length: 0,
                headers: Headers::default(),
                checksum: None,
                md5: None,
                trailing: None,
                metadata: Vec::new(),
            };
            let put = (Work::Put(putting), op);
            let (job, connection) = named_job(put, now, 7, (&submitter, &mut doorway), 4);
            host.push(job, now);
            connections.push(connection);
        }
        let mut stub = put_and_get_stub();
        // Told its driver wants its body, which has not come.
        assert_eq!(stepped(&mut host, &mut stub, &mut doorway, 1), [64]);
        let idle = stepped(&mut host, &mut stub, &mut doorway, 32);
        assert_eq!(idle.iter().sum::<u64>(), 0, "{idle:?}");
        // Each connection reads `Ready`, sends the body's end and names its put.
        let crc = checksum::checksum(Algorithm::Crc32c, &[]).unwrap();
        for connection in &mut connections {
            assert_eq!(
                connection.answers.try_recv().unwrap(),
                Some(door::Answer::Ready)
            );
            assert!(
                connection
                    .body
                    .try_send(Piece::End(Some(crc.clone())))
                    .is_ok()
            );
            connection.submitter.wake();
        }
        // Its end taken and its commit asked; then its commit answered, its reply given.
        assert_eq!(stepped(&mut host, &mut stub, &mut doorway, 2), [64, 64]);
        assert!(host.idle(), "every put ended");
        assert_eq!(stub.asked, 64, "each put's one commit");
        for connection in &mut connections {
            let answers = answered(&mut connection.answers);
            assert!(
                matches!(answers.as_slice(), [door::Answer::Head(reply), door::Answer::End]
                    if reply.status == Status::Done && reply.version == "v"),
                "{answers:?}"
            );
        }
    }

    /// Steps GETs waiting on their own reads alone: 64 gets of an empty object, each one's version
    /// read answered 32 turns after it is asked. Each get is stepped as it asks and as its answer
    /// comes, its head and end given and its job let go: the 31 turns between pass it by
    /// (`Job::rests`), whatever its door, so each get is stepped twice, 128 steps, where every turn
    /// stepped every get, 2,112 steps, 33 a get; and each answered its head and end.
    #[test]
    fn a_get_waiting_on_its_reads_alone_is_stepped_only_as_they_are_answered() {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(64, Some(std::sync::Arc::new(scheming_gateway())));
        let read = scheming_gateway().charges.read(1);
        let mut replies = Vec::new();
        for _ in 0..64 {
            let (job, _, answers) = local_job(work(get_op(), None), now, read);
            host.push(job, now);
            replies.push(answers);
        }
        let mut stub = put_and_get_stub();
        let mut held = Vec::new();
        let _ = clock_reads();
        let mut turns = 0;
        while !host.idle() {
            turns += 1;
            assert!(turns <= 64, "the gets never ended");
            if turns == 33 {
                stub.answers.append(&mut held);
            }
            host.step(&mut stub).unwrap();
            held.append(&mut stub.answers);
        }
        let (_, steps) = clock_reads();
        assert_eq!(turns, 33, "asked at the first turn, answered at the 33rd");
        assert_eq!(stub.asked, 64, "each get's one read");
        assert_eq!(
            steps,
            2 * 64,
            "each get stepped as it asks and as it is answered"
        );
        for answers in &mut replies {
            let answers = answered(answers);
            assert!(
                matches!(answers.as_slice(), [door::Answer::Head(reply), door::Answer::End]
                    if reply.status == Status::Done && reply.size == 0),
                "{answers:?}"
            );
        }
    }

    /// Runs 64 jobs of `work` through the S3 door on a stub answering each ask with `answer`, each
    /// answer given 32 turns after its ask, as a range's log answers many turns of a loaded node's
    /// task later: the turns they took, the steps, and each one's answers on its door.
    fn held_jobs(
        work: impl Fn() -> Work,
        answer: fn(&JobAsk) -> Outcome,
    ) -> (u64, u64, Vec<Vec<door::Answer>>) {
        const JOBS: usize = 64;
        const LATE: u64 = 32;
        let now = hyper_rt::futures::now_ns();
        let mut gateway = scheming_gateway();
        gateway.keys = vec![(1, WrappingKey::generate().unwrap())];
        let mut host = Host::new(JOBS, Some(std::sync::Arc::new(gateway)));
        let mut replies = Vec::new();
        for _ in 0..JOBS {
            let (job, _, answers) = local_job(work(), now, 1);
            host.push(job, now);
            replies.push(answers);
        }
        let mut stub = put_and_get_stub();
        stub.answer = answer;
        stub.table = Some(Table::empty(0));
        let mut held: VecDeque<(u64, (Pending, Outcome))> = VecDeque::new();
        let _ = clock_reads();
        let mut turns = 0;
        while !host.idle() {
            turns += 1;
            assert!(turns <= 1_000, "the jobs never ended");
            while held.front().is_some_and(|(asked, _)| asked + LATE <= turns) {
                let (_, answer) = held.pop_front().unwrap();
                stub.answers.push(answer);
            }
            host.step(&mut stub).unwrap();
            held.extend(stub.answers.drain(..).map(|answer| (turns, answer)));
        }
        let (_, steps) = clock_reads();
        (turns, steps, replies.iter_mut().map(answered).collect())
    }

    /// Whether `answers` are a reply of `status` and its end, the reply as `reply` holds.
    fn replied(answers: &[door::Answer], status: Status, reply: impl Fn(&Reply) -> bool) -> bool {
        matches!(answers, [door::Answer::Head(head), door::Answer::End]
            if head.status == status && reply(head))
    }

    /// 64 HeadBuckets whose bucket's row is read 32 turns after it is asked take 128 steps, each
    /// stepped as it asks and as it is answered, where every turn stepped every one, 2,112, 33 a
    /// job; each answered `Done`.
    #[test]
    fn a_single_read_is_stepped_only_as_it_is_answered() {
        let (turns, steps, replies) = held_jobs(
            || Work::Read(Box::new(Single::HeadBucket("b".into()))),
            |ask| match ask {
                JobAsk::Read(_, Query::Bucket { .. }) => Outcome::Read(ReadOutcome::Read(
                    Box::new(Read::Bucket(Some(bucket_row()))),
                )),
                _ => panic!("an ask a HeadBucket does not make"),
            },
        );
        assert_eq!(turns, 33, "asked at the first turn, answered at the 33rd");
        assert_eq!(
            steps,
            2 * 64,
            "each stepped as it asks and as it is answered"
        );
        for answers in &replies {
            assert!(replied(answers, Status::Done, |_| true), "{answers:?}");
        }
    }

    /// 64 DeleteObjects of three keys each, its bucket's row read first, then one delete out at a
    /// time (the stub's one command, an operation's share), each answer 32 turns after its ask:
    /// 320 steps, five a job, its row and three deletes asked and its reply given, where every turn
    /// stepped every one, 8,256, 129 a job; each answered `Done` listing its three keys deleted.
    #[test]
    fn a_batch_delete_is_stepped_only_as_its_deletes_are_answered() {
        let (turns, steps, replies) = held_jobs(
            || {
                let objects: Vec<Target> = ["a", "b", "c"]
                    .into_iter()
                    .map(|key| Target {
                        key: key.into(),
                        version: None,
                        etag: None,
                    })
                    .collect();
                Work::Deletes(Box::new(Deleting {
                    bucket: "b".into(),
                    results: vec![None; objects.len()],
                    objects,
                    quiet: false,
                    row: None,
                    next: 0,
                    out: 0,
                    held: false,
                }))
            },
            |ask| match ask {
                JobAsk::Read(_, Query::Bucket { .. }) => Outcome::Read(ReadOutcome::Read(
                    Box::new(Read::Bucket(Some(bucket_row()))),
                )),
                JobAsk::Command(_, Command::Name(_)) => Outcome::Command(CommandOutcome::Answered(
                    Answer::Name(name::Outcome::Deleted {
                        marker: false,
                        version: None,
                    }),
                )),
                _ => panic!("an ask a DeleteObjects does not make"),
            },
        );
        assert_eq!(turns, 129, "its row and three deletes, each 32 turns");
        assert_eq!(
            steps,
            5 * 64,
            "each stepped as it asks and as it is answered"
        );
        for answers in &replies {
            assert!(
                replied(answers, Status::Done, |reply| matches!(
                    &reply.listing,
                    Some(Listing::Deleted(deleted)) if deleted.len() == 3
                        && deleted.iter().all(|d| d.status == Status::Done)
                )),
                "{answers:?}"
            );
        }
    }

    /// 64 CompleteMultipartUploads of one part, each ask answered 32 turns after it is asked: the
    /// bucket's row, then the driver's upload row, which finds none, then its Complete asked as a
    /// retry of one that may have committed, which the Name range answers `NoSuchUpload`: 256
    /// steps, four a job, where every turn stepped every one, 6,208, 97 a job; each answered
    /// `NoSuchUpload`, its driver's outcome.
    #[test]
    fn a_completion_is_stepped_only_as_its_driver_is_answered() {
        let (turns, steps, replies) = held_jobs(
            || {
                Work::Complete(Box::new(Completing {
                    bucket: "b".into(),
                    key: "k".into(),
                    upload: "0123456789ABC".into(),
                    listed: vec![(1, "0".repeat(32))],
                    native: None,
                    preconditions: Preconditions::default(),
                    completion: None,
                    waiting: VecDeque::new(),
                }))
            },
            |ask| match ask {
                JobAsk::Read(_, Query::Bucket { .. }) => Outcome::Read(ReadOutcome::Read(
                    Box::new(Read::Bucket(Some(bucket_row()))),
                )),
                JobAsk::Read(_, Query::Upload { .. }) => {
                    Outcome::Read(ReadOutcome::Read(Box::new(Read::Upload(None))))
                }
                JobAsk::Command(_, Command::Name(_)) => Outcome::Command(CommandOutcome::Answered(
                    Answer::Name(name::Outcome::NoSuchUpload),
                )),
                _ => panic!("an ask a completion of no upload does not make"),
            },
        );
        assert_eq!(
            turns, 97,
            "its row, its upload and its Complete, each 32 turns"
        );
        assert_eq!(
            steps,
            4 * 64,
            "each stepped as it asks and as it is answered"
        );
        for answers in &replies {
            assert!(
                replied(answers, Status::NoSuchUpload, |_| true),
                "{answers:?}"
            );
        }
    }

    /// 64 CopyObjects of an empty source, each read answered 32 turns after it is asked: the
    /// destination's bucket's row, the source's version, then its put's commit: 256 steps, four
    /// a job, where every turn stepped every one, 6,208, 97 a job; each answered `Done` with the
    /// version committed. And 64 whose source is gone take three steps each, 192, where every
    /// turn stepped every one, 4,160; each answered `NoSuchKey`.
    #[test]
    fn a_copy_is_stepped_only_as_its_reads_and_its_drivers_are_answered() {
        fn copy() -> Work {
            work(
                Op::Copy(Box::new(object::Copy {
                    bucket: "b".into(),
                    key: "k2".into(),
                    id: 1,
                    source: mantle_s3::copy::Source {
                        bucket: "b".into(),
                        key: "k".into(),
                        version: None,
                    },
                    conditions: Headers::default(),
                    headers: Headers::default(),
                    directive: Directive::Copy,
                    metadata: Vec::new(),
                    checksum: None,
                })),
                None,
            )
        }
        let (turns, steps, replies) = held_jobs(copy, |ask| match ask {
            JobAsk::Read(_, Query::Bucket { .. }) => Outcome::Read(ReadOutcome::Read(Box::new(
                Read::Bucket(Some(bucket_row())),
            ))),
            JobAsk::Read(_, Query::Current { .. }) => Outcome::Read(ReadOutcome::Read(Box::new(
                Read::Current(Some((1, empty_version()))),
            ))),
            JobAsk::Command(_, Command::Name(_)) => {
                Outcome::Command(CommandOutcome::Answered(Answer::Name(name::Outcome::Put {
                    version: mantle_meta::key::version_id(1),
                })))
            }
            _ => panic!("an ask a copy of an empty source does not make"),
        });
        assert_eq!(
            turns, 97,
            "its row, its source and its commit, each 32 turns"
        );
        assert_eq!(
            steps,
            4 * 64,
            "each stepped as it asks and as it is answered"
        );
        for answers in &replies {
            assert!(
                replied(answers, Status::Done, |reply| reply.version
                    == mantle_meta::key::version_id(1)),
                "{answers:?}"
            );
        }
        let (turns, steps, replies) = held_jobs(copy, |ask| match ask {
            JobAsk::Read(_, Query::Bucket { .. }) => Outcome::Read(ReadOutcome::Read(Box::new(
                Read::Bucket(Some(bucket_row())),
            ))),
            JobAsk::Read(_, Query::Current { .. }) => {
                Outcome::Read(ReadOutcome::Read(Box::new(Read::Current(None))))
            }
            _ => panic!("an ask a copy of no source does not make"),
        });
        assert_eq!(turns, 65, "its row and its source, each 32 turns");
        assert_eq!(
            steps,
            3 * 64,
            "each stepped as it asks and as it is answered"
        );
        for answers in &replies {
            assert!(replied(answers, Status::NoSuchKey, |_| true), "{answers:?}");
        }
    }

    /// 64 DeleteBuckets of a bucket gone, its row read for its owner 32 turns after it is asked,
    /// take 128 steps, where every turn stepped every one, 2,112; each answered `NoSuchBucket`.
    #[test]
    fn a_bucket_operation_is_stepped_only_as_it_is_answered() {
        let (turns, steps, replies) = held_jobs(
            || {
                Work::Bucket(Box::new(Coordinating {
                    bucket: "b".into(),
                    delete: true,
                    owned: false,
                    coordinator: None,
                }))
            },
            |ask| match ask {
                JobAsk::Read(_, Query::Bucket { .. }) => {
                    Outcome::Read(ReadOutcome::Read(Box::new(Read::Bucket(None))))
                }
                _ => panic!("an ask a DeleteBucket of no bucket does not make"),
            },
        );
        assert_eq!(turns, 33, "asked at the first turn, answered at the 33rd");
        assert_eq!(
            steps,
            2 * 64,
            "each stepped as it asks and as it is answered"
        );
        for answers in &replies {
            assert!(
                replied(answers, Status::NoSuchBucket, |_| true),
                "{answers:?}"
            );
        }
    }

    /// 64 DeleteObjects, each its bucket's row read and then its delete, each answered 32 turns
    /// after it is asked: 192 steps, three a job, where every turn stepped every one, 4,160, 65 a
    /// job; each answered `Done`. And 64 ListPrincipals, an operator's read of the root range: 128
    /// steps, where every turn stepped every one, 2,112; each answered `Done`.
    #[test]
    fn a_command_and_an_operators_request_are_stepped_only_as_they_are_answered() {
        let (turns, steps, replies) = held_jobs(
            || {
                Work::Command(Box::new(Commanding {
                    bucket: "b".into(),
                    then: Then::Delete {
                        key: "k".into(),
                        if_match: None,
                        named: None,
                    },
                    asked: false,
                    row: None,
                }))
            },
            |ask| match ask {
                JobAsk::Read(_, Query::Bucket { .. }) => Outcome::Read(ReadOutcome::Read(
                    Box::new(Read::Bucket(Some(bucket_row()))),
                )),
                JobAsk::Command(_, Command::Name(_)) => Outcome::Command(CommandOutcome::Answered(
                    Answer::Name(name::Outcome::Deleted {
                        marker: false,
                        version: None,
                    }),
                )),
                _ => panic!("an ask a DeleteObject does not make"),
            },
        );
        assert_eq!(turns, 65, "its row and its delete, each 32 turns");
        assert_eq!(
            steps,
            3 * 64,
            "each stepped as it asks and as it is answered"
        );
        for answers in &replies {
            assert!(replied(answers, Status::Done, |_| true), "{answers:?}");
        }
        let (turns, steps, replies) = held_jobs(
            || {
                Work::Admin(Box::new(Administering {
                    asking: Admin::Read {
                        query: Query::Principals {
                            after: None,
                            max: 10,
                        },
                        one: false,
                    },
                    asked: false,
                    issued: None,
                }))
            },
            |ask| match ask {
                JobAsk::Read(_, Query::Principals { .. }) => Outcome::Read(ReadOutcome::Read(
                    Box::new(Read::Principals(Principals::default())),
                )),
                _ => panic!("an ask a ListPrincipals does not make"),
            },
        );
        assert_eq!(turns, 33, "asked at the first turn, answered at the 33rd");
        assert_eq!(
            steps,
            2 * 64,
            "each stepped as it asks and as it is answered"
        );
        for answers in &replies {
            assert!(replied(answers, Status::Done, |_| true), "{answers:?}");
        }
    }

    /// A DeleteObject whose bucket's row comes while the node has no room for its delete keeps
    /// the row and asks the delete at its next turn with room, answered `Done`; where the delete
    /// was made, refused and never asked again, the job held no ask and no bound and was stepped
    /// for good (measured: no reply in 1,000 turns). It does not rest meanwhile.
    #[test]
    fn a_command_the_node_has_no_room_for_is_asked_at_a_later_turn() {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
        let (job, _, mut answers) = local_job(
            Work::Command(Box::new(Commanding {
                bucket: "b".into(),
                then: Then::Delete {
                    key: "k".into(),
                    if_match: None,
                    named: None,
                },
                asked: false,
                row: None,
            })),
            now,
            1,
        );
        host.push(job, now);
        let mut stub = put_and_get_stub();
        stub.answer = |ask| match ask {
            JobAsk::Read(_, Query::Bucket { .. }) => Outcome::Read(ReadOutcome::Read(Box::new(
                Read::Bucket(Some(bucket_row())),
            ))),
            JobAsk::Command(_, Command::Name(_)) => Outcome::Command(CommandOutcome::Answered(
                Answer::Name(name::Outcome::Deleted {
                    marker: false,
                    version: None,
                }),
            )),
            _ => panic!("an ask a DeleteObject does not make"),
        };
        host.step(&mut stub).unwrap();
        assert_eq!(stub.asked, 1, "its row asked");
        // Its row comes while the node holds as many commands as it keeps.
        stub.no_room = true;
        for _ in 0..4 {
            host.step(&mut stub).unwrap();
            assert!(!host.jobs[0].resting, "a job holding no ask does not rest");
        }
        assert_eq!(stub.asked, 1, "no command asked without room");
        stub.no_room = false;
        let mut turns = 0;
        while !host.idle() {
            turns += 1;
            assert!(turns <= 1_000, "the delete was never asked");
            host.step(&mut stub).unwrap();
        }
        assert_eq!(stub.asked, 2, "its row, then its delete");
        let answers = answered(&mut answers);
        assert!(replied(&answers, Status::Done, |_| true), "{answers:?}");
    }

    /// A native request's job of `work`, handed with its fields read (`handed_job`), named `name`
    /// to `host` where it has calls, as the node's task names one it takes: the node's task's half
    /// of its door, which names it as it moves the door (`Call`), beside it.
    fn called_job(host: &Host, work: Work, now: u64, charge: u64, name: u64) -> (Job, Replies) {
        let (mut job, mut replies) = handed_job(work, now, charge);
        if let Door::Native(_, native) = &mut job.door {
            native.name = Some(name);
            native
                .handed
                .name
                .store(name, std::sync::atomic::Ordering::Release);
        }
        replies.call_by(host.caller());
        (job, replies)
    }

    /// A native door's node's task's half, the exchange it writes to, and whether its reply is
    /// over.
    type Written = (Replies, Seen, bool);

    /// The steps each of `turns` turns of `host` took, the node's task writing each reply not over
    /// as far as its exchange takes it after the host's step, as `Node::write_replies` follows
    /// `Node::step_host`.
    fn native_turns(
        host: &mut Host,
        stub: &mut Stub,
        doors: &mut [Written],
        now: u64,
        turns: usize,
    ) -> Vec<u64> {
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        (0..turns)
            .map(|_| {
                let _ = clock_reads();
                host.step(stub).unwrap();
                let (_, steps) = clock_reads();
                for (replies, seen, over) in doors.iter_mut().filter(|(_, _, over)| !*over) {
                    *over = replies.write(seen, true, now, &mut moved, &mut slowed) == Wrote::Over;
                }
                steps
            })
            .collect()
    }

    /// 64 native GETs of an empty object whose exchanges take none of their replies' fields for
    /// 32 turns, then half take them, then the rest: the steps of each turn, every get answered
    /// `Done` with no bytes and its exchange ended. Named to their host where `calling`, as the
    /// node's task names each (`Call`); else, as before, a native job is never passed by for its
    /// door.
    fn native_gets(calling: bool) -> Vec<u64> {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(64, Some(std::sync::Arc::new(scheming_gateway())));
        if calling {
            host.calling(Calls::new(64, None));
        }
        let read = scheming_gateway().charges.read(1);
        let mut doors = Vec::new();
        for name in 0..64 {
            let (job, replies) = called_job(&host, work(get_op(), None), now, read, name);
            host.push(job, now);
            doors.push((replies, Seen::default(), false));
        }
        let mut stub = put_and_get_stub();
        // Its version asked; read, its head given; its head taken, its fields not.
        let mut steps = native_turns(&mut host, &mut stub, &mut doors, now, 3);
        // Its exchange takes nothing.
        steps.extend(native_turns(&mut host, &mut stub, &mut doors, now, 32));
        // Half the exchanges take their fields whole, the node's task naming their gets, which
        // give their ends; then the rest.
        for (_, seen, _) in doors.iter_mut().take(32) {
            seen.credit = usize::MAX;
        }
        steps.extend(native_turns(&mut host, &mut stub, &mut doors, now, 2));
        for (_, seen, _) in doors.iter_mut().skip(32) {
            seen.credit = usize::MAX;
        }
        steps.extend(native_turns(&mut host, &mut stub, &mut doors, now, 2));
        assert!(host.idle(), "every get ended");
        assert_eq!(stub.asked, 64, "each get's one read");
        for (_, seen, over) in &doors {
            assert!(*over && seen.ended && !seen.refused);
            let reply = reply_seen(seen);
            assert_eq!((reply.status, reply.size), (Status::Done, 0));
        }
        steps
    }

    /// Steps native GETs waiting on their exchanges: 64 gets of an empty object, each named to its
    /// host as the node's task takes or writes whole an answer of its reply (`Call`). Each get is
    /// stepped as it asks its version, as the version comes, its head given, and as the node's
    /// task takes its head, whose fields its exchange takes none of; then 32 turns pass every get
    /// by (`Job::rests`), where each turn stepped every get; and as each exchange takes its fields
    /// whole, its get alone is stepped, giving its end and ending. 256 steps over 39 turns, four a
    /// get, where every turn stepped every get, 2,432, 37 or 38 a get; each answered `Done` with
    /// no bytes and its exchange ended, no wake lost.
    #[test]
    fn a_native_get_waiting_on_its_exchange_is_stepped_only_as_the_node_names_it() {
        let named = native_gets(true);
        let walked = native_gets(false);
        assert_eq!(named.len(), 39);
        assert_eq!(named[..3], [64, 64, 64]);
        assert_eq!(named[3..35].iter().sum::<u64>(), 0, "{named:?}");
        assert_eq!(named[35..], [0, 32, 0, 32]);
        assert_eq!(named.iter().sum::<u64>(), 256);
        assert_eq!(walked.iter().sum::<u64>(), 2_432, "{walked:?}");
    }

    /// 64 native puts of an empty body whose bodies come 32 turns late: the steps of each turn,
    /// every put answered `Done` with its version. Named to their host where `calling`, as
    /// [`native_gets`].
    fn native_puts(calling: bool) -> Vec<u64> {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(64, Some(std::sync::Arc::new(scheming_gateway())));
        if calling {
            host.calling(Calls::new(64, None));
        }
        let mut doors = Vec::new();
        let mut feedings = Vec::new();
        for name in 0..64 {
            let mut putting = native_put(0);
            putting.trailing = None;
            let (job, mut replies) = called_job(&host, Work::Put(putting), now, 7, name);
            let handed = std::sync::Arc::clone(&replies.handed);
            let call = replies.call.clone();
            feedings.push(feed((0, None), replies.body.take().unwrap(), handed, call));
            host.push(job, now);
            let seen = Seen {
                credit: usize::MAX,
                ..Seen::default()
            };
            doors.push((replies, seen, false));
        }
        let mut stub = put_and_get_stub();
        // Its driver wants its body, which has not come; then 32 turns more.
        let mut steps = native_turns(&mut host, &mut stub, &mut doors, now, 33);
        // The node's task gives each body's end, naming its put: its end taken and its commit
        // asked; then its commit answered, its reply given.
        let crc = checksum::checksum(Algorithm::Crc32c, &[]).unwrap();
        for feeding in &mut feedings {
            assert_eq!(feeding.send(Piece::End(Some(crc.clone()))), Fed::Moved);
        }
        steps.extend(native_turns(&mut host, &mut stub, &mut doors, now, 2));
        assert!(host.idle(), "every put ended");
        assert_eq!(stub.asked, 64, "each put's one commit");
        for (_, seen, over) in &doors {
            assert!(*over && seen.ended && !seen.refused);
            let reply = reply_seen(seen);
            assert_eq!((reply.status, reply.version.as_str()), (Status::Done, "v"));
        }
        steps
    }

    /// Steps native puts waiting on their bodies: 64 puts of an empty body, each named to its host
    /// as the node's task gives a piece of its body (`Call`). Each put is stepped at its first
    /// turn, finding its body's channel empty; 32 turns then pass every put by (`Job::rests`),
    /// where every turn stepped every put; and as the node's task gives each body's end, each put
    /// alone is stepped, taking its end and asking its commit, and again as its commit is
    /// answered. 192 steps over 35 turns, three a put, where every turn stepped every put, 2,240,
    /// 35 a put; each answered `Done` with its version, no wake lost.
    #[test]
    fn a_native_put_waiting_on_its_body_is_stepped_only_as_the_node_names_it() {
        let named = native_puts(true);
        let walked = native_puts(false);
        assert_eq!(named.len(), 35);
        assert_eq!(named[0], 64);
        assert_eq!(named[1..33].iter().sum::<u64>(), 0, "{named:?}");
        assert_eq!(named[33..], [64, 64]);
        assert_eq!(walked.iter().sum::<u64>(), 2_240, "{walked:?}");
    }

    /// 64 native GETs of an empty object whose fields arrive in two halves, the second 32 turns
    /// after the first: the steps of each turn, every get answered `Done` with no bytes and its
    /// exchange ended. Each is woken as the rest arrives, as the node's task wakes a job on its
    /// exchange's `BodyReady` (`Host::arrived`); and named to its host where `calling`, else, as
    /// before, a job reading its fields is never passed by.
    fn native_fields(calling: bool) -> Vec<u64> {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(64, Some(std::sync::Arc::new(scheming_gateway())));
        if calling {
            host.calling(Calls::new(64, None));
        }
        let (head, fields) = get_op().encode().unwrap();
        let length = u64::try_from(fields.len()).unwrap();
        let half = fields.len() / 2;
        assert!(half > 0, "fields in two halves");
        let read = scheming_gateway().charges.read(1);
        let mut stub = put_and_get_stub();
        let mut doors = Vec::new();
        for name in 0..64 {
            let head = Head::decode(&head, Some(length)).unwrap();
            let work = Work::Fields(head, Vec::with_capacity(fields.len()));
            let (job, replies) = called_job(&host, work, now, read, name);
            host.push(job, now);
            stub.arrived.push((name, fields[..half].to_vec(), 0));
            let seen = Seen {
                credit: usize::MAX,
                ..Seen::default()
            };
            doors.push((replies, seen, false));
        }
        // Each reads the first half of its fields; then 32 turns more.
        let mut steps = native_turns(&mut host, &mut stub, &mut doors, now, 33);
        // The rest arrives, and the node's task wakes each job reading its fields: each reads them
        // whole and its get asks its version, read, its head given and written, its end given.
        for (_, arrived, _) in &mut stub.arrived {
            arrived.extend_from_slice(&fields[half..]);
        }
        for at in 0..host.jobs.len() {
            host.arrived(at);
        }
        steps.extend(native_turns(&mut host, &mut stub, &mut doors, now, 5));
        assert!(host.idle(), "every get ended");
        assert_eq!(stub.asked, 64, "each get's one read");
        for (_, seen, over) in &doors {
            assert!(*over && seen.ended && !seen.refused);
            let reply = reply_seen(seen);
            assert_eq!((reply.status, reply.size), (Status::Done, 0));
        }
        steps
    }

    /// Steps native jobs reading their fields: 64 GETs whose fields arrive in two halves 32 turns
    /// apart. Each is stepped at its first turn, reading the first half; 32 turns then pass every
    /// job by (`Job::rests`), where each turn stepped every job; and as the rest arrives each is
    /// woken (`Host::arrived`), reads its fields whole, then asks its version, gives its head as
    /// the version comes and its end as the head is written. 320 steps over 38 turns, five a job,
    /// where the rule before took 2,368, 37 a job, 33 of them reading its fields; each answered
    /// `Done` with no bytes and its exchange ended, no wake lost.
    #[test]
    fn a_native_job_reading_its_fields_is_stepped_only_as_they_arrive() {
        let named = native_fields(true);
        let walked = native_fields(false);
        assert_eq!(named.len(), 38);
        assert_eq!(named[0], 64);
        assert_eq!(named[1..33].iter().sum::<u64>(), 0, "{named:?}");
        assert_eq!(named[33..], [64, 64, 64, 64, 0]);
        assert_eq!(named.iter().sum::<u64>(), 320);
        assert_eq!(walked[..33], [64; 33], "{walked:?}");
        assert_eq!(walked.iter().sum::<u64>(), 2_368, "{walked:?}");
    }

    /// The node's task names a native job once until its host takes the name, however often it
    /// moves the job's door meanwhile; the host lets the name go before it steps the job, so a
    /// move after names it again; a job handed to another host is named there afresh; and a name
    /// past the calls' room is missed, on which the host wakes every native job, read once.
    #[test]
    fn the_node_names_a_native_job_once_until_its_host_takes_the_name() {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(2, Some(std::sync::Arc::new(scheming_gateway())));
        host.calling(Calls::new(1, None));
        let done = || Work::Draining(Box::new(Reply::status(Status::Done)));
        let (job, replies) = called_job(&host, done(), now, 0, 7);
        let handed = std::sync::Arc::clone(&replies.handed);
        assert!(handed.calls.load(std::sync::atomic::Ordering::Acquire));
        let call = replies.call.clone().unwrap();
        call.name(&handed);
        call.name(&handed);
        let calls = host.calls.as_mut().unwrap();
        assert_eq!(calls.taken.try_recv().unwrap(), Some(7));
        assert_eq!(calls.taken.try_recv().unwrap(), None, "named once");
        // Taken but not let go: named no more.
        call.name(&handed);
        assert_eq!(calls.taken.try_recv().unwrap(), None, "named no more");
        // Let go of as the host takes it before a step: the job woken, and named again after.
        handed
            .called
            .store(false, std::sync::atomic::Ordering::Release);
        host.push(job, now);
        host.lull(0);
        call.name(&handed);
        host.take_calls();
        assert!(!host.jobs[0].resting, "woken by its name");
        assert!(!handed.called.load(std::sync::atomic::Ordering::Acquire));
        call.name(&handed);
        host.take_calls();
        assert!(!handed.called.load(std::sync::atomic::Ordering::Acquire));
        // A third and fourth job's names past the room of two: missed, every native job woken.
        let (other, other_replies) = called_job(&host, done(), now, 0, 8);
        host.push(other, now);
        let (third, third_replies) = called_job(&host, done(), now, 0, 9);
        host.push(third, now);
        for at in 0..host.jobs.len() {
            host.lull(at);
        }
        for replies in [&replies, &other_replies, &third_replies] {
            replies.call.as_ref().unwrap().name(&replies.handed);
        }
        assert!(
            host.calls
                .as_ref()
                .unwrap()
                .caller
                .missed
                .load(std::sync::atomic::Ordering::Acquire)
        );
        host.take_calls();
        assert!(host.jobs.iter().all(|job| !job.resting), "every one woken");
        assert!(
            !host
                .calls
                .as_ref()
                .unwrap()
                .caller
                .missed
                .load(std::sync::atomic::Ordering::Acquire),
            "read once"
        );
        // Handed to another host with its word still set: let go as it is pushed there.
        handed
            .called
            .store(true, std::sync::atomic::Ordering::Release);
        let (native, _) = native_door().unwrap();
        let mut half = Box::new(native);
        half.handed = std::sync::Arc::clone(&handed);
        half.name = Some(7);
        let mut there = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
        there.take_handed(
            Box::new(Handover {
                half,
                principal: "o".into(),
                op: get_op(),
                block: None,
                deadline_ns: None,
                admitted_ns: None,
                charge: 0,
                window: 1,
                driven: Some(1),
            }),
            None,
        );
        assert!(!handed.called.load(std::sync::atomic::Ordering::Acquire));
    }

    /// A gateway of the rates and target `mantle init`'s cells state (crates/mantle/tests/support).
    fn scheming_gateway() -> GatewaySettings {
        GatewaySettings {
            keys: Vec::new(),
            handover_ns: 1,
            window: 1,
            objects: 1,
            objects_bytes: 1,
            cache_bytes: 0,
            charges: crate::derive::Charges::new(1, &[durability::Scheme::Copies(3)]).unwrap(),
            clients: 1,
            bucket_quota: 1,
            scan_rows: 1,
            rates: Rates {
                chunk: 0.000_001_552_511_415_525_114_2,
                domains: None,
                bursts: Vec::new(),
                repair: 0.041_666_666_666_666_664,
            },
            target: 1e-11,
        }
    }

    /// A volume in each of `domains`, in order.
    fn placed_in(domains: &[&str]) -> Vec<Placed> {
        domains
            .iter()
            .zip(1u128..)
            .map(|(domain, volume)| Placed {
                volume,
                node: u64::try_from(volume).unwrap(),
                domain: (*domain).to_owned(),
                bytes: 1 << 30,
            })
            .collect()
    }

    /// The scheming of a table read at `applied` over `standing` after `was`, settled as far as
    /// it goes without waiting, as `Node::table` reads it.
    fn read(
        was: Option<Scheming>,
        before: &[Placed],
        standing: &[Placed],
        applied: u64,
        gateway: &GatewaySettings,
    ) -> Scheming {
        let mut scheming = Scheming::after(was, same_domains(before, standing), applied);
        scheming.settle(Some(gateway), standing);
        scheming
    }

    /// The process's compute pool, started once for this test binary with one worker, so a test
    /// holding that worker holds the pool, and room for a job of each test at once; and the
    /// tests that use it one at a time, as each may hold its worker or fill its room. Each lets
    /// the pool go as it found it, every job it put there ended ([`Held`]).
    fn pool() -> std::sync::MutexGuard<'static, ()> {
        static STARTED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
        static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());
        STARTED.get_or_init(|| {
            compute::start(compute::Config {
                workers: 1,
                queued: 64,
                piece_ns: 1,
            })
            .unwrap();
        });
        ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The pool's one worker held in a job ([`hold_the_worker`]), and the jobs its test queued
    /// behind it that nothing else waits on. Dropped, as its test ends or unwinds, it lets the
    /// worker go and returns once each of those jobs ended, so the next test to take the pool
    /// finds its worker free and its room empty: a job gives its place in the room back before
    /// its end is sent (hyper-rt `compute`). Letting the worker go alone left the room to the
    /// worker's scheduling, below the test threads (hyper-rt `prioritysys`), on Windows at a
    /// level that runs only while no thread at theirs is ready: the next test asked for a place
    /// while the last one's jobs still held every one.
    struct Held {
        release: Option<std::sync::mpsc::SyncSender<()>>,
        jobs: Vec<OneshotReceiver<()>>,
    }

    impl Drop for Held {
        fn drop(&mut self) {
            drop(self.release.take());
            for job in &mut self.jobs {
                let _ = job.blocking_recv();
            }
        }
    }

    /// Holds the pool's one worker in a job until the returned [`Held`] is dropped: returns once
    /// the worker is in it.
    fn hold_the_worker() -> Held {
        let (release, released) = std::sync::mpsc::sync_channel::<()>(0);
        let (entered, inside) = std::sync::mpsc::sync_channel::<()>(1);
        let job = compute::run(move || {
            entered.send(()).unwrap();
            let _ = released.recv();
        })
        .expect("the pool takes the job that holds its worker");
        inside.recv().unwrap();
        Held {
            release: Some(release),
            jobs: vec![job],
        }
    }

    /// The volume table read again after an entry that left its volumes and their domains as
    /// they were takes the scheme chosen before, asking nothing of the allocator; one whose
    /// domains changed is chosen again (research/71, step 5: the choice ran at each entry the
    /// root range applied, 2,448.5 allocations each time).
    #[test]
    fn the_scheme_is_chosen_again_only_when_the_tables_domains_change() {
        let gateway = scheming_gateway();
        let first = placed_in(&["a", "b", "c"]);
        let chosen = read(None, &[], &first, 1, &gateway);
        assert!(chosen.settled && chosen.choosing.is_none());
        let scheme = chosen.now;
        // Three wide: two domains do not fit it, so a table of two chooses again at once.
        assert!(
            matches!(scheme, Ok(Some(s)) if s.width() == 3),
            "{scheme:?}"
        );
        let same = placed_in(&["a", "b", "c"]);
        hyper_measure::alloc::begin();
        let carried = read(Some(chosen), &first, &same, 2, &gateway);
        let counts = hyper_measure::alloc::end();
        assert_eq!(carried.now, scheme);
        assert_eq!((counts.allocations, counts.reallocations), (0, 0));
        // One domain fewer: chosen again, over two domains.
        let fewer = placed_in(&["a", "b", "b"]);
        hyper_measure::alloc::begin();
        let again = read(Some(carried), &same, &fewer, 3, &gateway);
        let counts = hyper_measure::alloc::end();
        assert!(again.settled && again.choosing.is_none());
        assert_eq!(again.now, Ok(Ask::of(&gateway, &fewer).choose()));
        assert_eq!((counts.allocations, counts.reallocations), (1_833, 3));
    }

    /// The scheme the model chooses on the compute pool is the one it chooses inline, at every
    /// count of domains a candidate fits, and over the tables above; a table whose domains grew
    /// asks the pool, its scheme held meanwhile, and takes the pool's answer once it landed.
    #[test]
    fn the_scheme_chosen_on_the_pool_is_the_one_chosen_inline() {
        let _pool = pool();
        let gateway = scheming_gateway();
        let names = [
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o",
        ];
        for count in 1..=names.len() {
            let standing = placed_in(&names[..count]);
            let inline = Ask::of(&gateway, &standing).choose();
            let ask = Ask::of(&gateway, &standing);
            let mut answer = compute::run(move || ask.choose()).unwrap();
            assert_eq!(answer.blocking_recv().unwrap(), inline, "{count} domains");
        }
        // Two domains, then three: the scheme of two fits three, so the choice goes to the pool.
        let two = placed_in(&["a", "b"]);
        let three = placed_in(&["a", "b", "c"]);
        let first = read(None, &[], &two, 1, &gateway);
        let held = first.now;
        let mut grown = read(Some(first), &two, &three, 2, &gateway);
        assert!(!grown.settled);
        assert_eq!(grown.now, held, "the scheme held until the answer lands");
        let mut choosing = grown.choosing.take().unwrap();
        assert_eq!(choosing.asked_at, 2);
        let landed = choosing.answer.blocking_recv().unwrap();
        assert_eq!(landed, Ask::of(&gateway, &three).choose());
        // Its answer, as it lands, is taken for the domains it was asked for.
        let (sender, answer) = hyper_rt::sync::oneshot().unwrap();
        sender.send(landed).unwrap();
        grown.choosing = Some(Choosing {
            asked_at: 2,
            answer,
        });
        grown.settle(Some(&gateway), &three);
        assert!(grown.settled && grown.choosing.is_none());
        assert_eq!(grown.now, Ok(landed));
    }

    /// An answer asked for domains the table has since left is dropped, the scheme held kept,
    /// and the choice asked again for the domains the table has; a choice that unwound is a
    /// typed refusal of the scheme until the domains change.
    #[test]
    fn a_stale_answer_is_dropped_and_the_choice_asked_again() {
        let _pool = pool();
        let gateway = scheming_gateway();
        let two = placed_in(&["a", "b"]);
        let three = placed_in(&["a", "b", "c"]);
        let four = placed_in(&["a", "b", "c", "d"]);
        let first = read(None, &[], &two, 1, &gateway);
        let held = first.now;
        let worker = hold_the_worker();
        // Grown to three at 2, the choice waiting behind the held worker; grown to four at 3
        // before it ran: the one choice still runs, now stale.
        let grown = read(Some(first), &two, &three, 2, &gateway);
        let mut again = read(Some(grown), &three, &four, 3, &gateway);
        assert_eq!(again.choosing.as_ref().map(|c| c.asked_at), Some(2));
        assert_eq!(again.now, held);
        // Its answer for three domains lands: not taken, and a choice asked for four.
        drop(worker);
        let mut stale = again.choosing.take().unwrap();
        let answer = stale.answer.blocking_recv().unwrap();
        let (sender, landed) = hyper_rt::sync::oneshot().unwrap();
        sender.send(answer).unwrap();
        again.choosing = Some(Choosing {
            asked_at: 2,
            answer: landed,
        });
        again.settle(Some(&gateway), &four);
        assert_eq!(again.now, held, "the stale answer dropped");
        assert!(!again.settled);
        let mut fresh = again.choosing.take().unwrap();
        assert_eq!(fresh.asked_at, 3);
        assert_eq!(
            fresh.answer.blocking_recv().unwrap(),
            Ask::of(&gateway, &four).choose()
        );
        // A choice for the current domains whose job unwound: its sender gone without a value.
        let (sender, unwound) = hyper_rt::sync::oneshot::<Option<durability::Scheme>>().unwrap();
        drop(sender);
        again.choosing = Some(Choosing {
            asked_at: 3,
            answer: unwound,
        });
        again.settle(Some(&gateway), &four);
        assert!(again.settled && again.choosing.is_none());
        assert_eq!(again.now, Err(Unwound));
        let table = Table {
            applied: 3,
            placed: four.clone(),
            facts: Vec::new(),
            standing: four,
            scheming: again,
        };
        assert!(matches!(table.scheme(), Err(NodeError::Runtime(_))));
    }

    /// A choice the pool refuses, at its bound here, is made at once on the shard, as it was
    /// before the pool: the table never waits on a choice nobody runs.
    #[test]
    fn a_refused_choice_is_made_at_once_on_the_shard() {
        let _pool = pool();
        let gateway = scheming_gateway();
        let two = placed_in(&["a", "b"]);
        let three = placed_in(&["a", "b", "c"]);
        let first = read(None, &[], &two, 1, &gateway);
        let mut worker = hold_the_worker();
        // The pool's room filled behind its held worker, a job at a time until one is refused.
        for _ in 0..64 {
            match compute::run(|| ()) {
                Ok(job) => worker.jobs.push(job),
                Err(refused) => {
                    assert!(
                        matches!(refused.error, hyper_rt::RtError::Capacity { .. }),
                        "{:?}",
                        refused.error
                    );
                    break;
                }
            }
        }
        let grown = read(Some(first), &two, &three, 2, &gateway);
        drop(worker);
        assert!(grown.settled && grown.choosing.is_none());
        assert_eq!(grown.now, Ok(Ask::of(&gateway, &three).choose()));
    }

    /// A reading whose domains changed under a scheme that still fits them returns before the
    /// model runs: with the pool's worker held, the choice cannot have run, yet the reading is
    /// done, its scheme the one held; and the shard did a fraction of the model's work,
    /// allocating less than the model alone does inline.
    #[test]
    fn the_shard_is_not_held_while_the_model_chooses() {
        let _pool = pool();
        let gateway = scheming_gateway();
        let two = placed_in(&["a", "b"]);
        let three = placed_in(&["a", "b", "c"]);
        let first = read(None, &[], &two, 1, &gateway);
        let held = first.now;
        hyper_measure::alloc::begin();
        let inline = Ask::of(&gateway, &three).choose();
        let model = hyper_measure::alloc::end();
        let worker = hold_the_worker();
        hyper_measure::alloc::begin();
        let grown = read(Some(first), &two, &three, 2, &gateway);
        let shard = hyper_measure::alloc::end();
        assert_eq!(grown.now, held);
        assert!(!grown.settled);
        let mut choosing = grown.choosing.unwrap();
        assert!(choosing.answer.try_recv().unwrap().is_none(), "not run yet");
        assert!(
            shard.allocations < model.allocations,
            "the shard's {shard:?} against the model's {model:?}"
        );
        drop(worker);
        assert_eq!(choosing.answer.blocking_recv().unwrap(), inline);
    }

    /// A write is charged at the widest window from the floor's to the one learned whose charge
    /// the operations' bytes hold: one learned past them is held to what they hold, so the write
    /// is admitted alone and never waits at the door for good; with room, the learned one stands.
    #[test]
    fn a_writes_window_is_held_to_what_the_operations_bytes_hold() {
        let charges = || crate::derive::Charges::new(1, &[durability::Scheme::Copies(3)]).unwrap();
        let length = 64u64 << 20;
        let learned = usize::try_from(length.div_ceil(charges().largest_block())).unwrap();
        let mut gateway = GatewaySettings {
            keys: Vec::new(),
            handover_ns: 1,
            window: 1,
            objects: 1,
            objects_bytes: crate::derive::most_charged(&charges()),
            cache_bytes: 0,
            charges: charges(),
            clients: 1,
            bucket_quota: 1,
            scan_rows: 1,
            rates: Rates {
                chunk: 0.000_001_552_511_415_525_114_2,
                domains: None,
                bursts: Vec::new(),
                repair: 0.041_666_666_666_666_664,
            },
            target: 1e-11,
        };
        let charge = |window| charges().write(window, length);
        assert!(charge(learned) > gateway.objects_bytes);
        let window = fitting(&gateway, learned, charge);
        assert!((1..learned).contains(&window), "{window} of {learned}");
        assert!(charge(window) <= gateway.objects_bytes);
        assert!(charge(window + 1) > gateway.objects_bytes);
        gateway.objects_bytes = charge(learned);
        assert_eq!(fitting(&gateway, learned, charge), learned);
    }

    /// A get is admitted at the floor's window, its object's size unknown (`sized`), and once its
    /// row is read holds the window the node learned of its gets (`reading`, `crate::window`): the
    /// floor's until it measured them, as many blocks as its clients take while one is read once it
    /// has, fewer for a slower client, no more than its range is cut into, and never wider than the
    /// operations' bytes hold beside the others'; charged for what its driver holds with it, its
    /// blocks being read at the largest block and the one taken to be written out
    /// (`get::most_bytes`). A put's measures leave it alone.
    #[test]
    fn a_gets_window_is_learned_and_charged_for_what_it_holds() {
        let scheme = durability::Scheme::Copies(3);
        let charges = || crate::derive::Charges::new(1, &[scheme]).unwrap();
        let layout = Layout::new(scheme).unwrap();
        let block = charges().largest_block();
        let mut gateway = GatewaySettings {
            keys: Vec::new(),
            handover_ns: 1,
            window: 1,
            objects: 1,
            objects_bytes: charges().read(64),
            cache_bytes: 0,
            charges: charges(),
            clients: 1,
            bucket_quota: 1,
            scan_rows: 1,
            rates: Rates {
                chunk: 0.000_001_552_511_415_525_114_2,
                domains: None,
                bursts: Vec::new(),
                repair: 0.041_666_666_666_666_664,
            },
            target: 1e-11,
        };
        let mut learned = crate::window::Windows::default();
        let admitted = sized(Some(&gateway), &learned, Moves::Read);
        assert_eq!(admitted, (charges().read(1), 1));
        let all = |_: u64| true;
        let long = 1u64 << 30;
        assert_eq!(reading(&gateway, &learned.get, long, admitted.0, all), 1);
        // Blocks read in 80 ms, a client taking 1 GB/s: 80 MB, ten blocks of 8,325,104 bytes.
        learned.get.took(80_000_000);
        learned.get.paced(1_000_000_000, 1_000_000_000);
        assert_eq!(sized(Some(&gateway), &learned, Moves::Read), admitted);
        let window = reading(&gateway, &learned.get, long, admitted.0, all);
        assert_eq!(window, 10);
        let charge = charges().read(window);
        assert!(charge >= get::most_bytes(window, &layout).unwrap() + block);
        assert!(charge > charges().read(window - 1));
        // No more than its range is cut into.
        assert_eq!(
            reading(&gateway, &learned.get, 3 * block, admitted.0, all),
            3
        );
        assert_eq!(reading(&gateway, &learned.get, 1, admitted.0, all), 1);
        // A put of a gigabyte has measured nothing of its own: the floor's.
        assert_eq!(sized(Some(&gateway), &learned, Moves::Written(long)).1, 1);
        // Held to what the operations' bytes hold, and to what they hold beside the others'.
        gateway.objects_bytes = charges().read(4);
        assert_eq!(reading(&gateway, &learned.get, long, admitted.0, all), 4);
        gateway.objects_bytes = charges().read(64);
        let room = charges().read(6) - admitted.0;
        let beside = |more: u64| more <= room;
        assert_eq!(reading(&gateway, &learned.get, long, admitted.0, beside), 6);
        let none = |more: u64| more == 0;
        assert_eq!(reading(&gateway, &learned.get, long, admitted.0, none), 1);
        // A slower client: 100 MB/s, 8 MB while a block is read, one block.
        for _ in 0..64 {
            learned.get.paced(100_000_000, 1_000_000_000);
        }
        assert_eq!(reading(&gateway, &learned.get, long, admitted.0, all), 1);
        // A head reads no bytes: the most, at the floor's window.
        assert_eq!(
            sized(Some(&gateway), &learned, Moves::Neither),
            (charges().of(None), 1)
        );
    }

    /// A copy is admitted at the floor's one block for its get and for its put, its source's size
    /// unknown until its version is read: charged for both at once, its get's block being read and
    /// its put's going down of the largest body a copy writes, whatever the node learned; never
    /// past the most one operation is charged, which the operations' bytes always hold.
    #[test]
    fn a_copy_is_admitted_at_the_floor_charged_for_its_get_and_its_put_at_once() {
        let scheme = durability::Scheme::Copies(3);
        let charges = || crate::derive::Charges::new(1, &[scheme]).unwrap();
        let gateway = GatewaySettings {
            keys: Vec::new(),
            handover_ns: 1,
            window: 1,
            objects: 1,
            objects_bytes: crate::derive::most_charged(&charges()),
            cache_bytes: 0,
            charges: charges(),
            clients: 1,
            bucket_quota: 1,
            scan_rows: 1,
            rates: Rates {
                chunk: 0.000_001_552_511_415_525_114_2,
                domains: None,
                bursts: Vec::new(),
                repair: 0.041_666_666_666_666_664,
            },
            target: 1e-11,
        };
        let source = mantle_s3::copy::Source {
            bucket: "b".into(),
            key: "k".into(),
            version: None,
        };
        let copy = Op::Copy(Box::new(object::Copy {
            bucket: "b".into(),
            key: "k2".into(),
            id: 1,
            source: source.clone(),
            conditions: Headers::default(),
            headers: Headers::default(),
            directive: Directive::Copy,
            metadata: Vec::new(),
            checksum: None,
        }));
        let part = Op::CopyPart(Box::new(object::CopyPart {
            bucket: "b".into(),
            key: "k2".into(),
            upload: "0123456789ABC".into(),
            number: 1,
            id: 1,
            source,
            conditions: Headers::default(),
        }));
        let floor = charges().read(1) + charges().write(1, mantle_s3::body::MAX_UPLOAD);
        let mut learned = crate::window::Windows::default();
        assert_eq!(charged(Some(&gateway), &learned, &copy), (floor, 1));
        assert_eq!(charged(Some(&gateway), &learned, &part), (floor, 1));
        // Wide windows learned of the node's puts and gets: a copy is still admitted at the floor.
        learned.put.took(80_000_000);
        learned.put.paced(1_000_000_000, 1_000_000_000);
        learned.get.took(80_000_000);
        learned.get.paced(1_000_000_000, 1_000_000_000);
        assert_eq!(charged(Some(&gateway), &learned, &copy), (floor, 1));
        assert!(floor <= crate::derive::most_charged(&charges()));
        // Any other operation is charged as what it moves says.
        let get = Op::Get {
            bucket: "b".into(),
            key: "k".into(),
            headers: Headers::default(),
            version: None,
        };
        assert_eq!(
            charged(Some(&gateway), &learned, &get),
            sized(Some(&gateway), &learned, Moves::Read)
        );
    }

    /// Once its source's version is read, a copy's put holds the window the node learned of its
    /// puts, and its get as many blocks as that put drains while one is read (`copy_windows`,
    /// `Windows::copy_get`): a slow put holds fewer of its source's blocks being read, a fast one
    /// more, at least one and no more than its span's blocks, and a copy of one block holds one
    /// for each. It is charged for both at once (`Charges::copy`): its admission takes the bytes
    /// past what it was admitted at, gives back those its span does not need, and widens no
    /// further than the operations' bytes hold, nor past what they hold beside the others'.
    #[test]
    fn a_copys_windows_follow_its_puts_pace_and_its_admission_what_they_hold() {
        let scheme = durability::Scheme::Copies(3);
        let charges = || crate::derive::Charges::new(1, &[scheme]).unwrap();
        let block = charges().largest_block();
        let long = 1u64 << 30;
        let mut gateway = GatewaySettings {
            keys: Vec::new(),
            handover_ns: 1,
            window: 1,
            objects: 1,
            objects_bytes: charges().copy(64, 64, long),
            cache_bytes: 0,
            charges: charges(),
            clients: 1,
            bucket_quota: 1,
            scan_rows: 1,
            rates: Rates {
                chunk: 0.000_001_552_511_415_525_114_2,
                domains: None,
                bursts: Vec::new(),
                repair: 0.041_666_666_666_666_664,
            },
            target: 1e-11,
        };
        let admitted = charges().copy(1, 1, mantle_s3::body::MAX_UPLOAD);
        let admission = |budget: u64, others: u64| {
            let mut admission = crate::admit::Admission::new(64, budget, 0);
            admission.admitted(others);
            admission.admitted(admitted);
            admission
        };
        // The bytes the admission holds, all of them: it holds no byte more.
        let holding = |admission: &crate::admit::Admission, budget: u64, held: u64| {
            admission.holds(budget - held) && !admission.holds(budget - held + 1)
        };
        let budget = gateway.objects_bytes;
        // As `Node::copy_source_read` fits them: the bytes past its charge taken, and those its
        // span does not need given back.
        let windows = |gateway: &GatewaySettings,
                       learned: &crate::window::Windows,
                       room: &mut crate::admit::Admission,
                       length: u64| {
            let fitted = copy_windows(gateway, learned, |more| room.holds(more), length, admitted);
            room.took(fitted.2.saturating_sub(admitted));
            room.gave(admitted.saturating_sub(fitted.2));
            fitted
        };
        let fitted = |gateway: &GatewaySettings, learned: &crate::window::Windows, length: u64| {
            let mut room = admission(gateway.objects_bytes, 0);
            windows(gateway, learned, &mut room, length)
        };

        // Nothing learned: one block each, and the bytes a gigabyte's copy does not need given back.
        let mut learned = crate::window::Windows::default();
        let mut room = admission(budget, 0);
        let (get, put, held) = windows(&gateway, &learned, &mut room, long);
        assert_eq!((get, put), (1, 1));
        assert_eq!(held, charges().copy(1, 1, long));
        assert!(held < admitted);
        assert!(holding(&room, budget, held));

        // Blocks read in 40 ms and recorded in 10 ms, no client's pace measured: the put holds the
        // floor's one block and drains it every 10 ms, so the get holds four being read.
        learned.get.took(40_000_000);
        learned.put.took(10_000_000);
        assert_eq!(fitted(&gateway, &learned, long).0, 4);
        // A slower put, its blocks recorded in 80 ms: one.
        let mut slower = learned;
        for _ in 0..64 {
            slower.put.took(80_000_000);
        }
        assert_eq!(fitted(&gateway, &slower, long).0, 1);

        // Its clients' puts paced at 1 GB/s: 10 MB while a block is recorded, two blocks of
        // 8,325,104 bytes going down, which the put drains every 10 ms, so the get reads eight; the
        // admission takes the bytes past the floor's.
        learned.put.paced(1_000_000_000, 1_000_000_000);
        let mut room = admission(budget, 0);
        let (get, put, held) = windows(&gateway, &learned, &mut room, long);
        assert_eq!((get, put), (8, 2));
        assert_eq!(held, charges().read(8) + charges().write(2, long));
        assert!(held > admitted);
        assert!(holding(&room, budget, held));
        // No more than its span's blocks, and a copy of one block holds one block for each.
        assert_eq!(
            fitted(&gateway, &learned, 3 * block),
            (3, 2, charges().copy(3, 2, 3 * block))
        );
        let small = 1 << 20;
        assert_eq!(
            fitted(&gateway, &learned, small),
            (1, 1, charges().copy(1, 1, small))
        );
        assert!(charges().copy(1, 1, small) < admitted);

        // Held to what the operations' bytes hold.
        gateway.objects_bytes = charges().copy(5, 2, long);
        assert_eq!(fitted(&gateway, &learned, long).0, 5);
        gateway.objects_bytes = budget;
        // And to what they hold beside the others': the room left for three blocks read.
        let others = budget - charges().copy(3, 2, long);
        let mut room = admission(budget, others);
        let (get, put, held) = windows(&gateway, &learned, &mut room, long);
        assert_eq!((get, put), (3, 2));
        assert!(holding(&room, budget, others + held));
        // With no room past what it was admitted at, the floor's, and the rest given back.
        let mut room = admission(budget, budget - admitted);
        let (get, put, held) = windows(&gateway, &learned, &mut room, long);
        assert_eq!((get, put), (1, 1));
        assert!(holding(&room, budget, budget - admitted + held));
    }

    /// A get blocked before reading its version must still be refused at its own deadline;
    /// once its reply began, expiry must only stop its notes, preserving the one reply. Neither
    /// its pending read nor notes from a peer extend its local bound (research/77 D2).
    #[test]
    fn a_held_get_stops_vouching_at_its_bound_before_or_after_its_reply() {
        let patience = u64::try_from(bounds::STOCK_PATIENCE.as_nanos()).unwrap();
        let (_body, body) = hyper_rt::sync::channel(1).unwrap();
        let (reply, _answers) = hyper_rt::sync::channel(1).unwrap();
        let mut job = Job {
            door: Door::Local(Box::new(Local {
                body: Some(Pieces {
                    pieces: body,
                    ended: None,
                    dry: false,
                }),
                reply,
                pending: VecDeque::new(),
                ready: false,
                mark: None,
            })),
            principal: "client".into(),
            work: getting("b".into(), "k".into(), Headers::default(), None, false),
            pending: vec![(Pending::Read(1), None)],
            deadline_ns: Some(10),
            moved_ns: 1,
            vouched_ns: None,
            resting: false,
            admitted_ns: Some(1),
            charge: 0,
            window: 1,
            driven: None,
            label: None,
        };
        assert!(job.held());
        assert_eq!(job.bounded(9), Bounded::Held);
        assert_eq!(job.bounded(10), Bounded::SlowDown, "no reply has begun");
        assert_eq!(job.due_ns(9, 2), Some(10), "wake the S3 job at its bound");

        let Work::Get(getting) = &mut job.work else {
            panic!("the job is a get");
        };
        getting.get =
            Some(Get::new(Some(1), 1, 0..1, Box::new(RootKeys { keys: Vec::new() }), 1).unwrap());
        assert_eq!(job.bounded(9), Bounded::Held);
        assert_eq!(job.bounded(10), Bounded::Unvouched, "its reply has begun");
        assert_eq!(job.bounded(u64::MAX), Bounded::Unvouched);
        assert_eq!(job.due_ns(9, 2), Some(10));
        assert_eq!(
            job.due_ns(10, 2),
            None,
            "an expired reply needs no vouch beat"
        );

        job.deadline_ns = None;
        assert_eq!(job.bounded(patience), Bounded::Held);
        assert_eq!(job.bounded(patience + 1), Bounded::Unvouched);
        // Actual bytes or an answered ask advance the idle bound; a note does not.
        job.moved_ns = patience;
        assert_eq!(job.bounded(patience + 1), Bounded::Held);
        job.deadline_ns = Some(patience + 1);
        assert_eq!(job.bounded(patience + 1), Bounded::Unvouched);
    }

    /// A native put's work as its fields name it, nothing read past them, and a driver made over a
    /// body of `length` bytes whose trailer is a CRC-32C (`Op::Put`'s `trailing`), which its end is
    /// checked against.
    fn native_put(length: u64) -> Box<Putting> {
        let op = Op::Put {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            length,
            headers: Headers::default(),
            checksum: None,
            md5: None,
            trailing: Some(Algorithm::Crc32c),
            metadata: Vec::new(),
        };
        let Work::Put(mut putting) = work(op, None) else {
            panic!("a put's work");
        };
        let root = WrappingKey::generate().unwrap();
        let data = DataKey::generate().unwrap();
        let keys = Keys {
            file: 1,
            wrapped: WrappedKey {
                by: Wrapper::Root(1),
                bytes: data.wrap(&root).unwrap(),
            },
            data,
        };
        let commit = Commit::Object(name::Put {
            bucket: "b".into(),
            incarnation: 1,
            key: "k".into(),
            versioning: Versioning::Unversioned,
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
                owner: "o".into(),
                headers: Vec::new(),
                retention: None,
                legal_hold: None,
                listing: None,
            },
            default: None,
            id: 1,
            deadline_ns: u64::MAX,
        });
        let body = put::Body {
            length,
            checksum: Some(Algorithm::Crc32c),
            content_md5: false,
        };
        let layout = Layout::new(durability::Scheme::Copies(3)).unwrap();
        let holding = Holding {
            handover_ns: 1,
            window: 1,
        };
        putting.put =
            Some(Put::new(commit, body, keys, layout, holding, Box::new(Random), 0).unwrap());
        putting
    }

    /// A body past a native put's fields: `length` bytes of the object, then its CRC-32C, the
    /// trailer's value.
    fn native_body(length: usize) -> (Vec<u8>, Vec<u8>) {
        let object: Vec<u8> = (0..length)
            .map(|i| u8::try_from((i * 7 + i / 251) % 256).unwrap())
            .collect();
        let trailer = checksum::checksum(Algorithm::Crc32c, &object)
            .unwrap()
            .bytes;
        (object, trailer)
    }

    /// A native put's feeding over a channel and words of its own, and the job's end of the
    /// channel: as a door lends them, its body's sender and what its halves share.
    fn fed(putting: &Putting) -> (Feeding, Pieces) {
        let (sender, pieces) = hyper_rt::sync::channel(door::Depth::FIRST).unwrap();
        let handed = std::sync::Arc::new(Handed::default());
        (
            feed((putting.length, putting.trailing), sender, handed, None),
            Pieces {
                pieces,
                ended: None,
                dry: false,
            },
        )
    }

    /// The node's task's half of a stream (`Node::feed_body`) over `stream`, the bytes its
    /// exchange delivers after the fields, as far as the put's job takes them, the job's turn
    /// (`take_body`) between each of the node's: each read is of a piece at most, and only once
    /// the job let go of the last. Whether the body ended.
    fn feed_and_take(
        feeding: &mut Feeding,
        pieces: &mut Pieces,
        putting: &mut Putting,
        stream: &mut &[u8],
    ) -> Result<bool, Status> {
        let mut moved = 0;
        for _ in 0..stream.len() / piece_len() + 8 {
            if feeding.send_held() == Some(Fed::Still) {
            } else if feeding.read_whole() {
                assert_eq!(feeding.end(), Fed::Over);
            } else if feeding.free() {
                let (read, rest) = stream.split_at(stream.len().min(piece_len()));
                feeding.give(read, &mut moved)?;
                *stream = rest;
            }
            take_body(pieces, None, putting, &mut moved, |_| {})?;
            if putting.length == u64::MAX {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// A native put's body, read past its fields by the node's task and given on its job's channel
    /// a piece at a time, reaches its driver in order and whole: each piece from the body's first
    /// byte, the trailer's value carried by the end, against which the driver's CRC-32C of the
    /// bytes it took holds. A trailer that does not hold is the driver's refusal.
    #[test]
    fn a_native_bodys_pieces_reach_its_put_in_order_and_whole() {
        let (object, trailer) = native_body(3 * piece_len() + 1_000);
        let past: Vec<u8> = object.iter().chain(&trailer).copied().collect();
        let mut putting = native_put(object.len() as u64);
        let (mut feeding, mut pieces) = fed(&putting);
        let mut stream = &past[..];
        assert_eq!(
            feed_and_take(&mut feeding, &mut pieces, &mut putting, &mut stream),
            Ok(true)
        );
        assert!(stream.is_empty());
        let put = putting.put.as_ref().unwrap();
        assert!(put.outcome().is_none(), "{:?}", put.outcome());
        assert_eq!(
            putting.expected.checksum.as_ref().map(|c| c.bytes.clone()),
            Some(trailer.clone())
        );

        // The object's bytes and the trailer's value in one read: the bytes given as a piece, the
        // trailer's value kept for the end.
        let (object, trailer) = native_body(10);
        let past: Vec<u8> = object.iter().chain(&trailer).copied().collect();
        let mut putting = native_put(10);
        let (mut feeding, mut pieces) = fed(&putting);
        let mut stream = &past[..];
        assert_eq!(
            feed_and_take(&mut feeding, &mut pieces, &mut putting, &mut stream),
            Ok(true)
        );
        assert!(putting.put.as_ref().unwrap().outcome().is_none());

        // A trailer that does not hold the bytes: the driver refuses the body.
        let (object, mut trailer) = native_body(piece_len() + 5);
        trailer[0] ^= 1;
        let past: Vec<u8> = object.iter().chain(&trailer).copied().collect();
        let mut putting = native_put(object.len() as u64);
        let (mut feeding, mut pieces) = fed(&putting);
        let mut stream = &past[..];
        assert_eq!(
            feed_and_take(&mut feeding, &mut pieces, &mut putting, &mut stream),
            Ok(true)
        );
        assert!(matches!(
            putting.put.as_ref().unwrap().outcome(),
            Some(Err(PutError::BadChecksum))
        ));
    }

    /// While a native put's job takes nothing, the node's task reads nothing more of its stream:
    /// one piece on the channel, and the next read only once the job let go of it, in the same
    /// storage, so nothing the body holds grows and the stream's flow control holds the client.
    #[test]
    fn a_full_channel_holds_the_streams_bytes() {
        let (object, _) = native_body(4 * piece_len());
        let putting = native_put(object.len() as u64);
        let (mut feeding, mut pieces) = fed(&putting);
        let mut moved = 0;
        assert!(feeding.free());
        assert_eq!(
            feeding.give(&object[..piece_len()], &mut moved),
            Ok(Fed::Moved)
        );
        // The job holds the piece on its channel: nothing more is read.
        assert!(!feeding.free());
        assert!(!feeding.free());
        let Ok(Some(Piece::Bytes(first, _))) = pieces.pieces.try_recv() else {
            panic!("the first piece");
        };
        assert_eq!(&first[..], &object[..piece_len()]);
        // Taken but not let go: still nothing.
        assert!(!feeding.free());
        let storage = first.as_ptr();
        drop(first);
        assert!(feeding.free());
        assert_eq!(
            feeding.give(&object[piece_len()..2 * piece_len()], &mut moved),
            Ok(Fed::Moved)
        );
        let Ok(Some(Piece::Bytes(second, _))) = pieces.pieces.try_recv() else {
            panic!("the second piece");
        };
        assert_eq!(&second[..], &object[piece_len()..2 * piece_len()]);
        assert_eq!(second.as_ptr(), storage, "the piece's storage is reused");
        assert_eq!(feeding.left, 2 * piece_len() as u64);
        // A short piece leaves room in the storage past it, which is not read into while the job
        // holds the piece.
        drop(second);
        assert!(feeding.free());
        assert_eq!(
            feeding.give(
                &object[2 * piece_len()..2 * piece_len() + 1_000],
                &mut moved
            ),
            Ok(Fed::Moved)
        );
        assert!(!feeding.free());
        let Ok(Some(Piece::Bytes(short, _))) = pieces.pieces.try_recv() else {
            panic!("the short piece");
        };
        assert!(!feeding.free());
        drop(short);
        assert!(feeding.free());
        // A body shorter than a piece is given in storage of its size.
        let putting = native_put(6);
        let (mut feeding, _pieces) = fed(&putting);
        assert_eq!(feeding.give(&[2; 6], &mut moved), Ok(Fed::Moved));
        assert_eq!(feeding.storage.as_ref().map(|(_, made)| *made), Some(6));
        // An end the channel has no room for is held, and given once the job takes the piece.
        let putting = native_put(1);
        let (mut feeding, mut pieces) = fed(&putting);
        assert_eq!(feeding.give(&[7, 1, 2, 3, 4], &mut moved), Ok(Fed::Moved));
        assert!(feeding.read_whole());
        assert_eq!(feeding.end(), Fed::Still);
        assert_eq!(feeding.send_held(), Some(Fed::Still));
        assert!(matches!(
            pieces.pieces.try_recv(),
            Ok(Some(Piece::Bytes(..)))
        ));
        assert_eq!(feeding.send_held(), Some(Fed::Over));
        assert!(matches!(
            pieces.pieces.try_recv(),
            Ok(Some(Piece::End(Some(_))))
        ));
    }

    /// The node's task's waker as a put's job on a driving shard holds it, the job's feeding with
    /// it: at each wake, whether the feeding may read the next piece then (`Feeding::free`).
    struct Freeing {
        feeding: std::sync::Mutex<Feeding>,
        free: std::sync::Mutex<Vec<bool>>,
    }

    impl std::task::Wake for Freeing {
        fn wake(self: std::sync::Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            let free = self.feeding.lock().unwrap().free();
            self.free.lock().unwrap().push(free);
        }
    }

    /// A put's job on a driving shard wakes the node's task for a piece of its body it took only
    /// once it let go of it (node.md §2.9 step 6f): the node's task reads the next into the
    /// piece's storage, once no piece holds it (`Feeding::free`), so a wake while the job held it
    /// would turn the node's task to find nothing to read, its next piece waiting on another word.
    #[test]
    fn a_driving_shards_put_wakes_the_node_once_it_let_go_of_a_piece() {
        let (object, _) = native_body(2 * piece_len());
        let putting = native_put(object.len() as u64);
        let (length, trailing) = (putting.length, putting.trailing);
        let (mut job, mut replies) = handed_job(Work::Put(putting), 0, 0);
        let Door::Native(_, native) = &mut job.door else {
            panic!("a native door");
        };
        let handed = std::sync::Arc::clone(&native.handed);
        let feeding = feed(
            (length, trailing),
            replies.body.take().unwrap(),
            handed,
            None,
        );
        let freeing = std::sync::Arc::new(Freeing {
            feeding: std::sync::Mutex::new(feeding),
            free: std::sync::Mutex::new(Vec::new()),
        });
        native.node = Some(std::task::Waker::from(std::sync::Arc::clone(&freeing)));
        let mut moved = 0;
        assert_eq!(
            freeing
                .feeding
                .lock()
                .unwrap()
                .give(&object[..piece_len()], &mut moved),
            Ok(Fed::Moved)
        );
        let mut host = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
        host.push(job, 0);
        host.step(&mut Stub::chunks()).unwrap();
        let free = freeing.free.lock().unwrap();
        assert_eq!(free.first(), Some(&true), "woken while it held the piece");
    }

    /// A native put's stream that stops or is reset ends its job as it did when the job read the
    /// stream: refused `IncompleteBody`, told on its channel or by its channel's end, the node's
    /// task gone with the exchange (`Node::job_lost`). A job that lets go of its body has the rest
    /// read and dropped by the node's task, nothing more given.
    #[test]
    fn a_stopped_or_reset_stream_ends_its_put_as_before() {
        let (object, _) = native_body(2 * piece_len());
        // The exchange failed mid-body: the node's task tells the job.
        let mut putting = native_put(object.len() as u64);
        let (feeding, mut pieces) = fed(&putting);
        feeding
            .pieces
            .try_send(Piece::Refused(Status::IncompleteBody))
            .unwrap();
        let mut moved = 0;
        assert_eq!(
            take_body(&mut pieces, None, &mut putting, &mut moved, |_| {}),
            Err(Status::IncompleteBody)
        );
        // The exchange lost and its feeding let go with it, the body unfinished.
        let mut putting = native_put(object.len() as u64);
        let (mut feeding, mut pieces) = fed(&putting);
        assert_eq!(
            feeding.give(&object[..piece_len()], &mut moved),
            Ok(Fed::Moved)
        );
        drop(feeding);
        assert_eq!(
            take_body(&mut pieces, None, &mut putting, &mut moved, |_| {}),
            Err(Status::IncompleteBody)
        );
        assert_eq!(putting.taken, piece_len() as u64);
        // The job let go of its body: the next piece read finds it gone, and the rest is dropped.
        let putting = native_put(object.len() as u64);
        let (mut feeding, pieces) = fed(&putting);
        drop(pieces);
        assert!(feeding.free());
        assert_eq!(
            feeding.give(&object[..piece_len()], &mut moved),
            Ok(Fed::Moved)
        );
        assert!(feeding.dropping);
    }

    /// What a native body's piece asks of the allocator on its way to the job, against what the
    /// job's own read of its exchange asked (research/71, step 5): none past the first piece's
    /// storage either way. A read was copied once into the bytes the job carried and once into its
    /// driver; now it is copied once into the feeding's storage, which the job's piece is a view
    /// of, and once into its driver, so a piece is copied no more than it was.
    #[test]
    fn a_native_bodys_piece_is_copied_and_allocated_no_more_than_the_jobs_own_read() {
        const PIECES: usize = 64;
        let (object, _) = native_body(PIECES * piece_len());
        let mut driver = Vec::with_capacity(piece_len());
        // The job's own read of its exchange: each piece copied into its carry, then into its
        // driver, the carry drained of what the driver took.
        let mut carry: Vec<u8> = Vec::new();
        let mut was = |read: &[u8], driver: &mut Vec<u8>| {
            carry.extend_from_slice(read);
            driver.clear();
            driver.extend_from_slice(&carry);
            carry.drain(..);
        };
        was(&object[..piece_len()], &mut driver);
        hyper_measure::alloc::begin();
        for read in object.chunks(piece_len()).skip(1) {
            was(read, &mut driver);
        }
        let before = hyper_measure::alloc::end();
        // The node's task's half: each piece copied into the feeding's storage and handed over;
        // the job copies it into its driver and lets it go.
        let putting = native_put(object.len() as u64);
        let (mut feeding, mut pieces) = fed(&putting);
        let mut moved = 0;
        let mut now = |read: &[u8], driver: &mut Vec<u8>| {
            assert!(feeding.free());
            assert_eq!(feeding.give(read, &mut moved), Ok(Fed::Moved));
            let Ok(Some(Piece::Bytes(piece, _))) = pieces.pieces.try_recv() else {
                panic!("a piece");
            };
            driver.clear();
            driver.extend_from_slice(&piece);
            piece.as_ptr()
        };
        // The first piece makes the storage, and the view that shares it: what the counter sees.
        hyper_measure::alloc::begin();
        let storage = now(&object[..piece_len()], &mut driver);
        let first = hyper_measure::alloc::end();
        assert!(first.allocations > 0, "{first:?}");
        hyper_measure::alloc::begin();
        let mut views = 0;
        for read in object.chunks(piece_len()).skip(1) {
            views += usize::from(now(read, &mut driver) == storage);
        }
        let after = hyper_measure::alloc::end();
        assert_eq!(views, PIECES - 1, "each piece is a view of the one storage");
        assert_eq!(
            (before.allocations, before.reallocations),
            (after.allocations, after.reallocations)
        );
        assert_eq!((after.allocations, after.reallocations), (0, 0));
    }

    /// A native exchange as a test sees it: the head and body length it was answered with, the
    /// bytes written to it and where each write's bytes lay, its end or refusal, and its asker told
    /// the work is held; taking `credit` bytes of body a turn, as its stream's flow control would.
    #[derive(Default)]
    struct Seen {
        answered: Option<(Vec<u8>, u64)>,
        body: Vec<u8>,
        writes: Vec<*const u8>,
        ended: bool,
        refused: bool,
        refusal: Option<Refusal>,
        working: usize,
        credit: usize,
        gone: bool,
    }

    impl Exchange for Seen {
        fn reply(&mut self, head: &[u8], body: u64) -> crate::node::Replied {
            assert!(self.answered.is_none(), "answered twice");
            self.answered = Some((head.to_vec(), body));
            if self.gone {
                crate::node::Replied::Ended
            } else {
                crate::node::Replied::Sent
            }
        }

        fn write(&mut self, bytes: &Bytes) -> Option<usize> {
            if self.gone {
                return None;
            }
            assert!(self.answered.is_some(), "a body before its head");
            let took = bytes.len().min(self.credit);
            self.credit -= took;
            self.body.extend_from_slice(&bytes[..took]);
            self.writes.push(bytes.as_ptr());
            Some(took)
        }

        fn end(&mut self) {
            self.ended = true;
        }

        fn refuse(&mut self, refusal: Refusal) {
            self.refused = true;
            self.refusal = Some(refusal);
        }

        fn working(&mut self) {
            self.working += 1;
        }
    }

    /// A get's driver as a test gives it: its pieces, in order, then whether it ended whole.
    struct Pieced {
        pieces: VecDeque<Bytes>,
        whole: Option<bool>,
    }

    impl Source for Pieced {
        fn take(&mut self) -> Option<Bytes> {
            self.pieces.pop_front()
        }

        fn ended(&self) -> Option<bool> {
            if self.pieces.is_empty() {
                self.whole
            } else {
                None
            }
        }
    }

    /// The pieces of a get of `count` pieces of `len` bytes each, each its own storage.
    fn pieces(count: usize, len: usize) -> VecDeque<Bytes> {
        (0..count)
            .map(|p| {
                Bytes::from(
                    (0..len)
                        .map(|i| u8::try_from((p * 31 + i * 7) % 251).unwrap())
                        .collect::<Vec<u8>>(),
                )
            })
            .collect()
    }

    /// A native get's reply opened with `fields`, its range `total` bytes, given as `send_held`
    /// gives it.
    fn open_get(native: &mut Native, fields: &[u8], total: u64) {
        let opening = Opening {
            head: b"head".to_vec(),
            fields: Bytes::copy_from_slice(fields),
            body: total,
            slowed: None,
            bound_ns: u64::MAX,
            admitted: true,
        };
        assert_eq!(native.give(Told::Head(Box::new(opening))), Sent::Taken);
    }

    /// A node's turn for a native get, as `Node::step_host` and `Node::write_replies` run one: the
    /// job's step, then the node's task's write, the job giving the next piece within the turn
    /// while the exchange takes more, at most `rounds` times. The job's turn's end and whether the
    /// reply is over.
    fn get_turn(
        native: &mut Native,
        replies: &mut Replies,
        source: &mut Pieced,
        (written, total): (&mut u64, u64),
        seen: &mut Seen,
        rounds: usize,
    ) -> (Gave, bool) {
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        let mut gave = give_native(native, source, written, total);
        for _ in 0..rounds {
            match replies.write(seen, false, 0, &mut moved, &mut slowed) {
                Wrote::Over => return (gave, true),
                Wrote::Still | Wrote::Draining | Wrote::Room => return (gave, false),
                Wrote::More => gave = give_native(native, source, written, total),
            }
        }
        (gave, false)
    }

    /// A native get's reply, given by its job on its channel and written by the node's task, reaches
    /// its exchange whole and in order: its head with the body's length, fields and range both, its
    /// fields, each piece, then its end; as many pieces a turn as the exchange takes, as when its job
    /// wrote its exchange. A reply with no bytes past its fields waits for the request's body, then
    /// goes whole and ends the exchange, a `SlowDown` counted for its cause.
    #[test]
    fn a_native_gets_reply_reaches_its_exchange_whole_and_in_order() {
        let fields = b"the reply's fields";
        let given = pieces(5, 3 * piece_len() + 17);
        let total: u64 = given.iter().map(|p| p.len() as u64).sum();
        let expected: Vec<u8> = fields
            .iter()
            .copied()
            .chain(given.iter().flat_map(|p| p.iter().copied()))
            .collect();
        // An exchange that takes 5,000 bytes a turn: the reply goes in as many turns as that takes.
        let (mut native, mut replies) = native_door().unwrap();
        let mut source = Pieced {
            pieces: given.clone(),
            whole: Some(true),
        };
        open_get(&mut native, fields, total);
        let mut seen = Seen::default();
        let mut written = 0;
        let mut turns = 0;
        loop {
            turns += 1;
            seen.credit = 5_000;
            let (_, over) = get_turn(
                &mut native,
                &mut replies,
                &mut source,
                (&mut written, total),
                &mut seen,
                given.len() + 2,
            );
            if over {
                break;
            }
            assert!(turns < 1_000);
        }
        assert_eq!(
            seen.answered,
            Some((b"head".to_vec(), fields.len() as u64 + total))
        );
        assert_eq!(seen.body, expected);
        assert!(seen.ended && !seen.refused);
        assert_eq!(turns, expected.len().div_ceil(5_000));
        assert_eq!(native.moved, total, "the job counts the range's bytes");
        // An exchange that takes everything: the whole reply in one turn, its end with it.
        let (mut native, mut replies) = native_door().unwrap();
        let mut source = Pieced {
            pieces: given.clone(),
            whole: Some(true),
        };
        open_get(&mut native, fields, total);
        let mut seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        let mut written = 0;
        let (gave, over) = get_turn(
            &mut native,
            &mut replies,
            &mut source,
            (&mut written, total),
            &mut seen,
            given.len() + 2,
        );
        assert_eq!((gave, over), (Gave::Over, true));
        assert_eq!(seen.body, expected);
        assert!(seen.ended);
        // A driver that fails past the head: the exchange ends unfinished.
        let (mut native, mut replies) = native_door().unwrap();
        let mut source = Pieced {
            pieces: pieces(1, 10),
            whole: Some(false),
        };
        open_get(&mut native, fields, 100);
        let mut seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        let (_, over) = get_turn(
            &mut native,
            &mut replies,
            &mut source,
            (&mut 0, 100),
            &mut seen,
            4,
        );
        assert!(over && seen.refused && !seen.ended);
        assert_eq!(seen.body.len(), fields.len() + 10);
        // A `SlowDown` waits for the request's body, then goes whole, counted for its cause.
        let (mut native, mut replies) = native_door().unwrap();
        let (head, reply_fields) = Reply::slowed(door::Slowed::Queued).encode().unwrap();
        let opening = Opening {
            head: head.clone(),
            fields: Bytes::from(reply_fields.clone()),
            body: 0,
            slowed: Some(door::Slowed::Queued),
            bound_ns: u64::MAX,
            admitted: true,
        };
        assert_eq!(native.offer(Told::Reply(Box::new(opening))), Sent::Taken);
        let mut seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Draining
        );
        assert!(seen.answered.is_none());
        assert_eq!(
            replies.write(&mut seen, true, 0, &mut moved, &mut slowed),
            Wrote::Over
        );
        assert_eq!(seen.answered, Some((head, reply_fields.len() as u64)));
        assert_eq!(seen.body, reply_fields);
        assert!(seen.ended);
        assert_eq!(moved, reply_fields.len() as u64);
        assert_eq!(slowed[door::Slowed::Queued.index()], 1);
    }

    /// A reply's channel holds one answer: while the node's task has not written what its job gave,
    /// the job takes nothing more from its driver, and an answer past the channel's room waits in
    /// its door, in order, until the node's task took the one before; nothing grows.
    #[test]
    fn a_full_reply_channel_holds_the_job() {
        let (mut native, mut replies) = native_door().unwrap();
        let given = pieces(3, piece_len());
        let mut source = Pieced {
            pieces: given.clone(),
            whole: Some(true),
        };
        open_get(&mut native, b"fields", 3 * piece_len() as u64);
        // Its fields not yet written: the job takes no piece.
        let mut written = 0;
        let total = 3 * piece_len() as u64;
        assert_eq!(
            give_native(&mut native, &mut source, &mut written, total),
            Gave::Waits
        );
        assert_eq!(source.pieces.len(), 3);
        // An exchange that takes nothing: the fields wait in the node's task, the job holds.
        let mut seen = Seen::default();
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Still
        );
        for _ in 0..4 {
            assert_eq!(
                give_native(&mut native, &mut source, &mut written, total),
                Gave::Waits
            );
        }
        assert_eq!(source.pieces.len(), 3);
        assert!(native.pending.is_empty());
        // Room for the fields and half a piece: one piece taken, and no other while it is unwritten.
        seen.credit = b"fields".len() + piece_len() / 2;
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::More
        );
        assert_eq!(
            give_native(&mut native, &mut source, &mut written, total),
            Gave::Waits
        );
        assert_eq!(source.pieces.len(), 2);
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Still
        );
        assert_eq!(
            give_native(&mut native, &mut source, &mut written, total),
            Gave::Waits
        );
        assert_eq!(source.pieces.len(), 2);
        // An answer past the channel's one waits in the door, in order, and goes once the node's
        // task took the one before.
        let (mut native, mut replies) = native_door().unwrap();
        assert_eq!(
            native.offer(Told::Bytes(Bytes::from_static(b"a"))),
            Sent::Taken
        );
        assert_eq!(native.offer(Told::End), Sent::Held);
        assert_eq!(native.pending.len(), 1);
        assert_eq!(native.flush(), Sent::Held);
        assert!(matches!(replies.told.try_recv(), Ok(Some(Told::Bytes(_)))));
        assert_eq!(native.flush(), Sent::Taken);
        assert!(matches!(replies.told.try_recv(), Ok(Some(Told::End))));
    }

    /// A job whose work is held says so on its door, and the node's task tells its exchange's asker
    /// at its next write, once for each time the job said it (research/77 D1).
    #[test]
    fn a_held_job_is_vouched_for_through_the_nodes_task() {
        let (native, mut replies) = native_door().unwrap();
        let mut seen = Seen::default();
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Still
        );
        assert_eq!(seen.working, 0);
        native.held();
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Still
        );
        assert_eq!(seen.working, 1);
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Still
        );
        assert_eq!(seen.working, 1, "said once, vouched once");
        native.held();
        native.held();
        replies.write(&mut seen, false, 0, &mut moved, &mut slowed);
        assert_eq!(seen.working, 2);
    }

    /// A stopped stream ends the job and its reply: an exchange gone as its reply is written is
    /// refused and let go by the node's task, and a job whose exchange's half was let go (refused,
    /// or ended unanswered, `Node::job_lost`) finds its door gone and ends. A job that let go with
    /// nothing given leaves its exchange's half nothing to write.
    #[test]
    fn a_stopped_stream_ends_the_job_and_its_reply() {
        let given = pieces(2, piece_len());
        // The exchange is gone as a piece is written.
        let (mut native, mut replies) = native_door().unwrap();
        let mut source = Pieced {
            pieces: given.clone(),
            whole: Some(true),
        };
        open_get(&mut native, b"fields", 2 * piece_len() as u64);
        let mut seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        let mut written = 0;
        let total = 2 * piece_len() as u64;
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::More
        );
        assert_eq!(
            give_native(&mut native, &mut source, &mut written, total),
            Gave::Waits
        );
        seen.gone = true;
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Over
        );
        assert!(seen.refused && !seen.ended);
        // The node's task lets go of it: the job's next answer finds its door gone.
        drop(replies);
        assert_eq!(
            give_native(&mut native, &mut source, &mut written, total),
            Gave::Over
        );
        // An exchange gone before its head is written.
        let (mut native, mut replies) = native_door().unwrap();
        open_get(&mut native, b"fields", 1);
        let mut seen = Seen {
            gone: true,
            ..Seen::default()
        };
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Over
        );
        // A job that let go with nothing more given: nothing to write.
        let (native, mut replies) = native_door().unwrap();
        drop(native);
        let mut seen = Seen::default();
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Over
        );
        assert!(seen.answered.is_none() && !seen.ended && !seen.refused);
    }

    /// A native get's pieces reach its exchange as its driver gave them, each write's bytes the
    /// piece's own storage (`Endpoint::write_body_bytes` keeps them by reference): no piece is
    /// copied on its way, where the job's own write copied each into QUIC's buffer
    /// (`Endpoint::write_body`); and past the first piece the way asks the allocator for nothing.
    #[test]
    fn a_native_replys_pieces_reach_the_exchange_uncopied() {
        const PIECES: usize = 64;
        let given = pieces(PIECES, piece_len());
        let storage: Vec<*const u8> = given.iter().map(|p| p.as_ptr()).collect();
        let (mut native, mut replies) = native_door().unwrap();
        let mut source = Pieced {
            pieces: given,
            whole: Some(true),
        };
        let total = (PIECES * piece_len()) as u64;
        open_get(&mut native, b"", total);
        let mut seen = Seen {
            body: Vec::with_capacity(PIECES * piece_len()),
            writes: Vec::with_capacity(PIECES + 1),
            credit: usize::MAX,
            ..Seen::default()
        };
        let mut written = 0;
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::More
        );
        // The first piece: what the channel and the door's queue set up once.
        assert_eq!(
            give_native(&mut native, &mut source, &mut written, total),
            Gave::Waits
        );
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::More
        );
        hyper_measure::alloc::begin();
        for _ in 1..PIECES {
            assert_eq!(
                give_native(&mut native, &mut source, &mut written, total),
                Gave::Waits
            );
            assert_eq!(
                replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
                Wrote::More
            );
        }
        let counts = hyper_measure::alloc::end();
        assert_eq!((counts.allocations, counts.reallocations), (0, 0));
        assert_eq!(
            seen.writes, storage,
            "each write is the piece's own storage"
        );
        assert_eq!(seen.body.len(), PIECES * piece_len());
        assert_eq!(
            give_native(&mut native, &mut source, &mut written, total),
            Gave::Over
        );
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Over
        );
        assert!(seen.ended);
    }

    /// One native request's door as the host and the node's task use it: taken as the request is,
    /// its job's end given and written, and both halves given back, the job's or the reply's first.
    /// What its two halves share, which tells the door.
    fn door_request(doors: &mut Doors, job_first: bool) -> *const Handed {
        let (mut native, mut replies) = doors.take().unwrap();
        assert!(!native.gone() && native.written());
        assert_eq!(native.offer(Told::End), Sent::Taken);
        let mut seen = Seen::default();
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Over
        );
        assert!(seen.ended && seen.working == 0);
        let handed = std::sync::Arc::as_ptr(&native.handed);
        if job_first {
            doors.give_native(native);
            doors.give_replies(replies);
        } else {
            doors.give_replies(replies);
            doors.give_native(native);
        }
        handed
    }

    /// A native request's door is the one a request before it let go of (node.md §2.9 step 6d):
    /// where each request made its own, its reply's channel and what its two halves share asking
    /// the allocator each time, a request past the first asks it for nothing, whichever half comes
    /// back first. A door given back with an answer its job gave that the node's task never took is
    /// emptied, and its words cleared, so the next request's job is not told its reply is gone, nor
    /// the node's task that its job ended. The host keeps no more doors than it holds exchanges.
    #[test]
    fn a_native_requests_door_is_kept_for_the_next_and_allocates_nothing() {
        const REQUESTS: usize = 64;
        hyper_measure::alloc::begin();
        let made = native_door().unwrap();
        let each = hyper_measure::alloc::end();
        drop(made);
        assert!(each.allocations > 0, "{each:?}");
        let mut doors = Doors::new(2);
        let first = door_request(&mut doors, true);
        hyper_measure::alloc::begin();
        let mut same = 0;
        for request in 0..REQUESTS {
            same += usize::from(door_request(&mut doors, request % 2 == 0) == first);
        }
        let kept = hyper_measure::alloc::end();
        assert_eq!((kept.allocations, kept.reallocations), (0, 0));
        assert_eq!(same, REQUESTS, "each request took the one door kept");
        // A job that gave past the node's task's letting go: its answer dropped with the door's
        // words, its next request's door as new.
        let (mut native, replies) = doors.take().unwrap();
        assert_eq!(
            native.give(Told::Bytes(Bytes::from_static(b"never written"))),
            Sent::Taken
        );
        native.held();
        doors.give_replies(replies);
        assert!(native.gone());
        assert_eq!(native.offer(Told::End), Sent::Gone);
        doors.give_native(native);
        let (mut native, mut replies) = doors.take().unwrap();
        assert!(!native.gone() && native.written() && native.pending.is_empty());
        let mut seen = Seen::default();
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Still
        );
        assert!(seen.answered.is_none() && seen.body.is_empty() && seen.working == 0);
        assert_eq!(native.offer(Told::End), Sent::Taken);
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Over
        );
        assert!(seen.ended);
        doors.give_native(native);
        doors.give_replies(replies);
        // Three out at once, the host holding two exchanges' doors: the third back is let go.
        let out: Vec<_> = (0..3).map(|_| doors.take().unwrap()).collect();
        for (native, replies) in out {
            doors.give_native(native);
            doors.give_replies(replies);
        }
        assert_eq!(doors.kept.len(), 2);
        assert!(doors.waiting.is_empty());
    }

    /// One native put's door as the host and the node's task use it for its body: taken as the
    /// request is, its body's sender lent to the put's feeding, which gives `pieces` pieces of
    /// `piece` and the body's end, each taken by the job from its door, then let go, its sender
    /// given back; the job's end given and written; and both halves given back. What its halves
    /// share, which tells the door.
    fn door_put(
        doors: &mut Doors,
        putting: &Putting,
        piece: &[u8],
        pieces: usize,
    ) -> *const Handed {
        let (mut native, mut replies) = doors.take().unwrap();
        let sender = replies.body.take().unwrap();
        let mut feeding = feed(
            (putting.length, putting.trailing),
            sender,
            std::sync::Arc::clone(&native.handed),
            None,
        );
        let mut moved = 0;
        for _ in 0..pieces {
            assert!(feeding.free());
            assert_eq!(feeding.give(piece, &mut moved), Ok(Fed::Moved));
            assert!(matches!(
                native.body.pieces.try_recv(),
                Ok(Some(Piece::Bytes(..)))
            ));
        }
        assert_eq!(feeding.end(), Fed::Over);
        assert!(matches!(
            native.body.pieces.try_recv(),
            Ok(Some(Piece::End(_)))
        ));
        replies.body = Some(feeding.let_go());
        assert_eq!(native.offer(Told::End), Sent::Taken);
        let mut seen = Seen::default();
        let (mut slowed, handed) = (
            [0; door::Slowed::ALL.len()],
            std::sync::Arc::as_ptr(&native.handed),
        );
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::Over
        );
        doors.give_native(native);
        doors.give_replies(replies);
        handed
    }

    /// A native put's body channel is its door's, kept with it for the next request (node.md §2.9
    /// step 6e): where each put made its own, a put past the first asks the allocator for nothing
    /// for its channels, its body's pieces and end given and taken through the door kept, the
    /// feeding's storage made and shared once a put and reused piece to piece as before. What the channel's
    /// ends' drops said is two words: a job that let go of its body has the rest dropped by its
    /// feeding, and a feeding let go tells its job the body did not come whole once it took the
    /// piece given before.
    #[test]
    fn a_native_puts_body_channel_is_kept_with_its_door_and_allocates_nothing() {
        const PUTS: usize = 64;
        let mut putting = native_put(0);
        putting.trailing = None;
        let mut doors = Doors::new(1);
        let first = door_put(&mut doors, &putting, &[], 0);
        hyper_measure::alloc::begin();
        let mut same = 0;
        for _ in 0..PUTS {
            same += usize::from(door_put(&mut doors, &putting, &[], 0) == first);
        }
        let kept = hyper_measure::alloc::end();
        assert_eq!((kept.allocations, kept.reallocations), (0, 0));
        assert_eq!(same, PUTS, "each put took the one door kept");
        // With a body: what the feeding's storage asks, as before, and nothing for the channels:
        // made at its first piece, and shared as that piece is split from it (`BytesMut::split`
        // promotes a storage made whole to one its views share), each piece past it a view.
        let mut putting = native_put(4 * piece_len() as u64);
        putting.trailing = None;
        let piece = vec![7; piece_len()];
        hyper_measure::alloc::begin();
        for _ in 0..PUTS {
            assert_eq!(door_put(&mut doors, &putting, &piece, 4), first);
        }
        let bodied = hyper_measure::alloc::end();
        assert_eq!(
            (bodied.allocations, bodied.reallocations),
            (2 * PUTS as u64, 0),
            "a storage a put, and its sharing"
        );

        // The job let go of its body: its feeding drops the rest.
        let (native, mut replies) = doors.take().unwrap();
        let mut feeding = feed(
            (putting.length, putting.trailing),
            replies.body.take().unwrap(),
            std::sync::Arc::clone(&native.handed),
            None,
        );
        native
            .handed
            .dropped
            .store(true, std::sync::atomic::Ordering::Release);
        let mut moved = 0;
        assert_eq!(feeding.give(&piece, &mut moved), Ok(Fed::Moved));
        assert!(feeding.dropping);
        replies.body = Some(feeding.let_go());
        doors.give_native(native);
        doors.give_replies(replies);

        // The node's task let go of the body before its end: the job takes the piece given before
        // it, then hears its body did not come whole.
        let (mut native, mut replies) = doors.take().unwrap();
        assert!(
            !native.handed.cut.load(std::sync::atomic::Ordering::Acquire),
            "a kept door's words are cleared"
        );
        let mut feeding = feed(
            (putting.length, putting.trailing),
            replies.body.take().unwrap(),
            std::sync::Arc::clone(&native.handed),
            None,
        );
        assert_eq!(feeding.give(&piece, &mut moved), Ok(Fed::Moved));
        replies.body = Some(feeding.let_go());
        let mut taking = native_put(4 * piece_len() as u64);
        taking.trailing = None;
        let Native {
            body,
            handed,
            moved: took,
            ..
        } = &mut *native;
        assert_eq!(
            take_body(body, Some(&handed.cut), &mut taking, took, |_| {}),
            Err(Status::IncompleteBody)
        );
        assert_eq!(taking.taken, piece_len() as u64, "the piece given before");
        doors.give_native(native);
        doors.give_replies(replies);
        assert_eq!(doors.kept.len(), 1);
    }

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
            refused(&work(part(name::MAX_PART + 1), None)),
            Some(Status::EntityTooLarge)
        );
        assert_eq!(refused(&work(part(name::MAX_PART), None)), None);
        let put = Op::Put {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            length: mantle_s3::body::MAX_UPLOAD + 1,
            headers: Headers::default(),
            checksum: None,
            md5: None,
            trailing: None,
            metadata: Vec::new(),
        };
        assert_eq!(refused(&work(put, None)), Some(Status::EntityTooLarge));
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
            metadata: Vec::new(),
        };
        assert_eq!(refused(&work(twice, None)), Some(Status::InvalidRequest));
        let create = |algorithm, full| Op::CreateUpload {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            checksum: Some((algorithm, full)),
            metadata: Vec::new(),
        };
        for (algorithm, full, ok) in [
            (Algorithm::Crc64Nvme, true, true),
            (Algorithm::Crc64Nvme, false, false),
            (Algorithm::Sha256, true, false),
            (Algorithm::Sha256, false, true),
            (Algorithm::Crc32c, true, true),
            (Algorithm::Crc32c, false, true),
        ] {
            let status = refused(&work(create(algorithm, full), None));
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

    /// What research/89 adds is refused from its request alone, before anything is read: a
    /// version ID mantle never makes (InvalidArgument), metadata past S3's 2 KB
    /// (MetadataTooLarge) or that no HTTP field carries, a part copy's number past 10,000.
    #[test]
    fn what_a_version_metadata_or_a_copy_names_is_refused_before_anything_is_read() {
        let get = |version: &str| Op::Get {
            bucket: "b".into(),
            key: "k".into(),
            headers: Headers::default(),
            version: Some(version.into()),
        };
        for named in ["null", "0123456789ABC"] {
            assert_eq!(refused(&work(get(named), None)), None, "{named}");
        }
        for invalid in ["", "nul", "0123456789AB", "0123456789ABCD", "0123456789ABU"] {
            assert_eq!(
                refused(&work(get(invalid), None)),
                Some(Status::InvalidVersion),
                "{invalid:?}"
            );
        }
        let delete = Op::Delete {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            headers: Headers::default(),
            version: Some("not one".into()),
        };
        assert_eq!(refused(&work(delete, None)), Some(Status::InvalidVersion));
        let put = |metadata: Metadata| Op::Put {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            length: 1,
            headers: Headers::default(),
            checksum: None,
            md5: None,
            trailing: None,
            metadata,
        };
        let at = vec![(
            "x-amz-meta-k".to_owned(),
            "v".repeat(mantle_s3::metadata::MAX_USER - 1),
        )];
        assert_eq!(refused(&work(put(at), None)), None);
        let past = vec![(
            "x-amz-meta-k".to_owned(),
            "v".repeat(mantle_s3::metadata::MAX_USER),
        )];
        assert_eq!(
            refused(&work(put(past), None)),
            Some(Status::MetadataTooLarge)
        );
        let control = vec![("x-amz-meta-k".to_owned(), "a\nb".to_owned())];
        assert_eq!(
            refused(&work(put(control), None)),
            Some(Status::InvalidArgument)
        );
        let source = mantle_s3::copy::Source {
            bucket: "b".into(),
            key: "k".into(),
            version: None,
        };
        let part = Op::CopyPart(Box::new(object::CopyPart {
            bucket: "b".into(),
            key: "k".into(),
            upload: "0123456789ABC".into(),
            number: name::MAX_PART_NUMBER + 1,
            id: 1,
            source: source.clone(),
            conditions: Headers::default(),
        }));
        assert_eq!(refused(&work(part, None)), Some(Status::InvalidArgument));
        let copy = Op::Copy(Box::new(object::Copy {
            bucket: "b".into(),
            key: "k2".into(),
            id: 1,
            source: mantle_s3::copy::Source {
                version: Some("bad".into()),
                ..source
            },
            conditions: Headers::default(),
            headers: Headers::default(),
            directive: Directive::Copy,
            metadata: Vec::new(),
            checksum: None,
        }));
        assert_eq!(refused(&work(copy, None)), Some(Status::InvalidVersion));
    }

    /// A node as a test serves the host it lends out (`Serve`, node.md §2.9 step 6d): each ask
    /// answered by `answer` and its outcome given at the host's next turn, and each job's end kept,
    /// with how many asks it let go. It holds no volume table, and reads no fields: its jobs come
    /// typed through the S3 door.
    pub(crate) struct Stub {
        pub(crate) answer: fn(&JobAsk) -> Outcome,
        pub(crate) answers: Vec<(Pending, Outcome)>,
        pub(crate) asked: u64,
        /// Whether it says it has no room for any ask (`Serve::room`).
        pub(crate) no_room: bool,
        /// The volume table it places a put's blocks by (`Serve::table`), where it holds one.
        table: Option<Table>,
        ended: Vec<(Option<u64>, u64, Option<usize>, usize)>,
        abandoned: usize,
        /// Whether it takes each ended job's door half, as a driving shard's lanes take it to the
        /// node's task, rather than giving it back for the host to keep, as the node does; and
        /// the last it took.
        takes_halves: bool,
        half: Option<Box<Native>>,
        /// What arrived of each native job's request, by the job's name, and how much of it the
        /// job read, as its exchange holds it (`Serve::read_fields`); none where its jobs come
        /// typed.
        arrived: Vec<(u64, Vec<u8>, usize)>,
    }

    impl Serve for Stub {
        fn ask(&mut self, ask: JobAsk) -> Result<Pending, NodeError> {
            if self.no_room {
                return Err(NodeError::Asks);
            }
            let pending = match &ask {
                JobAsk::Command(..) => Pending::Command(self.asked),
                JobAsk::Read(..) => Pending::Read(self.asked),
                JobAsk::Put { .. } | JobAsk::Get { .. } => Pending::Chunk(self.asked),
            };
            self.asked += 1;
            self.answers.push((pending, (self.answer)(&ask)));
            Ok(pending)
        }

        fn outcome(&mut self, pending: Pending) -> Option<Outcome> {
            let at = self.answers.iter().position(|(p, _)| *p == pending)?;
            Some(self.answers.swap_remove(at).1)
        }

        fn answers(&self) -> impl Iterator<Item = Pending> {
            self.answers.iter().map(|(pending, _)| *pending)
        }

        fn abandon(&mut self, asks: impl IntoIterator<Item = Pending>) {
            self.abandoned += asks.into_iter().count();
        }

        fn table(&mut self) -> Result<&Table, NodeError> {
            self.table
                .as_ref()
                .ok_or_else(|| NodeError::Bounds("a stub holds no volume table".into()))
        }

        fn group_of(&self, _: Layer) -> Option<u128> {
            Some(1)
        }

        fn ranges(&self) -> usize {
            1
        }

        fn command_room(&self) -> bool {
            !self.no_room
        }

        fn room(&self, _: &JobAsk) -> bool {
            !self.no_room
        }

        fn command_asks(&self) -> usize {
            1
        }

        fn wall_ns(&self) -> u64 {
            WALL_READS.with(|reads| reads.set(reads.get() + 1));
            0
        }

        fn freed(&mut self) -> bool {
            false
        }

        fn moved(&self) -> u64 {
            0
        }

        /// As `Node::read_fields`: what arrived of the job's fields read until a read takes none,
        /// and once whole, the work they name.
        fn read_fields(&mut self, job: &mut Job) -> Result<bool, NodeError> {
            let Door::Native(_, native) = &job.door else {
                panic!("a local door's work comes typed")
            };
            let name = native.name;
            let Work::Fields(head, fields) = &mut job.work else {
                return Ok(false);
            };
            let (_, arrived, read) = self
                .arrived
                .iter_mut()
                .find(|(of, ..)| Some(*of) == name)
                .expect("its request arrives");
            let wanted = usize::try_from(head.fields()).unwrap();
            let took = (arrived.len() - *read).min(wanted - fields.len());
            fields.extend_from_slice(&arrived[*read..*read + took]);
            *read += took;
            if fields.len() < wanted {
                return Ok(false);
            }
            job.work = work(head.op(fields).unwrap(), None);
            Ok(false)
        }

        fn took(&mut self, _: door::Paced) {}

        fn committed(&mut self, _: u64) {}

        fn filled(&mut self, _: u64, _: u64) {}

        fn read(&mut self, _: u64) {}

        fn windows(&self) -> crate::window::Windows {
            crate::window::Windows::default()
        }

        fn holds(&self, _: u64) -> bool {
            true
        }

        fn recharge(&mut self, _: u64, _: u64) {}

        fn job_ended(
            &mut self,
            admitted_ns: Option<u64>,
            charge: u64,
            driven: Option<usize>,
            asks: impl IntoIterator<Item = Pending>,
            half: Option<Box<Native>>,
        ) -> Option<Box<Native>> {
            self.ended
                .push((admitted_ns, charge, driven, asks.into_iter().count()));
            if !self.takes_halves {
                return half;
            }
            self.half = half;
            None
        }
    }

    impl Stub {
        /// A stub that answers each chunk ask `Stored`, its answers' room made for 4 at once.
        pub(crate) fn chunks() -> Self {
            Self {
                answer: |_| Outcome::Chunk(ChunkOutcome::Stored),
                answers: Vec::with_capacity(4),
                asked: 0,
                no_room: false,
                table: None,
                ended: Vec::with_capacity(4),
                abandoned: 0,
                takes_halves: false,
                half: None,
                arrived: Vec::new(),
            }
        }
    }

    impl Table {
        /// A table of no volumes read at `applied`, whose blocks take three copies.
        pub(crate) fn empty(applied: u64) -> Self {
            Self {
                applied,
                placed: Vec::new(),
                facts: Vec::new(),
                standing: Vec::new(),
                scheming: Scheming {
                    now: Ok(Some(durability::Scheme::Copies(3))),
                    domains_at: applied,
                    settled: true,
                    choosing: None,
                },
            }
        }
    }

    /// An object's version of no bytes, as its Name range holds it.
    fn empty_version() -> Version {
        Version {
            marker: false,
            null: false,
            modified_ns: 0,
            etag: "\"e\"".into(),
            size: 0,
            checksum: None,
            file: None,
            owner: "o".into(),
            headers: Vec::new(),
            retention: None,
            legal_hold: None,
            listing: None,
        }
    }

    /// A job of `work` through the S3 door, admitted at `now` and charged `charge`: its door's
    /// body's sender and its reply's receiver beside it.
    fn local_job(
        work: Work,
        now: u64,
        charge: u64,
    ) -> (Job, Sender<Piece>, ChannelReceiver<door::Answer>) {
        let (body_in, pieces) = hyper_rt::sync::channel(2).unwrap();
        let (reply, answers) = hyper_rt::sync::channel(4).unwrap();
        let job = Job {
            door: Door::Local(Box::new(Local {
                body: Some(Pieces {
                    pieces,
                    ended: None,
                    dry: false,
                }),
                reply,
                pending: VecDeque::new(),
                ready: false,
                mark: None,
            })),
            principal: "o".into(),
            work,
            pending: Vec::new(),
            deadline_ns: None,
            moved_ns: now,
            vouched_ns: None,
            resting: false,
            admitted_ns: Some(now),
            charge,
            window: 1,
            driven: None,
            label: None,
        };
        (job, body_in, answers)
    }

    /// A one-byte put of three copies, `offered` volumes offered its block (`0..offered`), and its
    /// chunks asked: each one's request, volume and key.
    fn put_of_copies(offered: u128) -> (Put, Vec<(u64, u128, mantle_chunk::ChunkKey)>) {
        let commit = Commit::Object(name::Put {
            bucket: "b".into(),
            incarnation: 1,
            key: "k".into(),
            versioning: Versioning::Unversioned,
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
                owner: "o".into(),
                headers: Vec::new(),
                retention: None,
                legal_hold: None,
                listing: None,
            },
            default: None,
            id: 0,
            deadline_ns: 0,
        });
        let data = DataKey::generate().unwrap();
        let bytes = data.wrap(&WrappingKey::generate().unwrap()).unwrap();
        let keys = Keys {
            file: 7,
            data,
            wrapped: WrappedKey {
                by: Wrapper::Root(1),
                bytes,
            },
        };
        let body = Body {
            length: 1,
            checksum: None,
            content_md5: false,
        };
        let holding = Holding {
            handover_ns: 1,
            window: 1,
        };
        let layout = Layout::new(durability::Scheme::Copies(3)).unwrap();
        let mut put = Put::new(commit, body, keys, layout, holding, Box::new(Random), 0).unwrap();
        assert_eq!(put.feed(b"x").unwrap(), 1);
        put.end(&Expected::default()).unwrap();
        let (id, place) = put.poll().unwrap();
        let put::Request::Place { width, .. } = place else {
            panic!("{place:?}");
        };
        assert_eq!(width, 3);
        put.answer(id, put::Answer::Volumes((0..offered).collect()))
            .unwrap();
        let chunks = std::iter::from_fn(|| put.poll())
            .map(|(id, request)| match request {
                put::Request::Chunk { volume, key, .. } => (id, volume, key),
                other => panic!("{other:?}"),
            })
            .collect();
        (put, chunks)
    }

    /// An S3 put's job, its driver `put`, its work last moved at 1.
    fn put_job(put: Put) -> Job {
        let mut putting = native_put(1);
        putting.put = Some(put);
        putting.taken = 1;
        local_job(Work::Put(putting), 1, 0).0
    }

    /// An S3 put whose chunks every volume refused for want of room is due at its beat, as a
    /// native one is, so the node turns to offer them room again (`Node::step_host`): it was due
    /// only at its client's patience, and nothing turned the node when a peer's volume freed room,
    /// so the fill suite's S3 put sat quiet (CI run 44454089741, linux-x86_64).
    #[test]
    fn an_s3_put_waiting_for_room_is_due_at_its_beat() {
        let (mut put, chunks) = put_of_copies(3);
        for (id, _, _) in chunks {
            put.answer(id, put::Answer::Busy).unwrap();
        }
        assert!(put.waits_for_room());
        let job = put_job(put);
        assert!(job.held() && job.waits_for_room());
        assert_eq!(job.due_ns(5, 2), Some(7), "the S3 door's put, at its beat");
    }

    /// The time from a peer's room freeing to a chunk it had no room for being written, on the
    /// asking side. Refused for room, as every peer refused one before it held it (`node::hold`),
    /// the chunk is asked again at its put's next beat, a probe timeout, about a second: room freed
    /// by the peer's other writers or its cleaner answers nothing of this node's, so nothing offers
    /// it room sooner (`Host::step`). Held on its exchange instead, it is never refused: its put
    /// waits on no room of its own, is due at its client's patience alone, and its answer, which the
    /// peer gives at the turn room frees, moves it at the host's next step.
    #[test]
    fn a_chunk_held_at_its_peer_is_answered_as_room_frees_and_one_refused_waits_its_beat() {
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap();
        // The peer's room frees just after its refusal reached the put.
        let freed = 1_000;
        let (mut put, chunks) = put_of_copies(3);
        put.answer(chunks[0].0, put::Answer::Busy).unwrap();
        let job = put_job(put);
        assert!(job.waits_for_room());
        assert_eq!(
            job.due_ns(freed, beat).map(|due| due - freed),
            Some(beat),
            "refused, its chunk is asked again a beat after the room freed"
        );
        let (put, chunks) = put_of_copies(3);
        let mut job = put_job(put);
        assert!(!job.waits_for_room(), "held, it waits for no room");
        assert_eq!(
            job.due_ns(freed, beat),
            Some(job.bound_ns()),
            "held, it is due at its client's patience alone"
        );
        // The peer writes it at the turn its room freed, and answers: the put moves on.
        let Work::Put(putting) = &mut job.work else {
            panic!("a put's job");
        };
        let put = putting.put.as_mut().unwrap();
        put.answer(chunks[0].0, put::Answer::Stored).unwrap();
        assert!(!put.waits_for_room() && put.outcome().is_none());
    }

    /// An S3 put whose chunk waits for room at its volume past two beats, its volume's cleaner
    /// then finding none, is answered `InsufficientStorage` while its client's body is unread, and
    /// its connection reads that answer. The connection, as the S3 listener runs it, reads the
    /// reply until the host wants the body, then sends the body a piece at a time as its channel
    /// has room, and reads the reply again only once the body is sent or its channel is closed
    /// (`listener::conn`'s `exchange` and `stream`), its channels at the depths it makes before
    /// any phase is measured, a piece and an answer (`door::Depth::FIRST`,
    /// `door::Replying::FIRST_ANSWERS`). A job that held its body until its reply and the reply's
    /// end both went, two answers to a channel of one, left the connection waiting for room in the
    /// body's channel, which the job no longer took, and the job waiting for room in the reply's,
    /// which the connection reads only once the body is sent: neither moved, and the fill suite's
    /// S3 put of its first object held for room went unanswered until its client gave it up
    /// (crates/mantle/tests/serve.rs; CI runs 44575349545, 44578467575 and 44581897076, the first
    /// bad commit 9a312b2b, which learns the reply's depth from one rather than holding two).
    #[test]
    fn an_s3_put_answered_while_its_body_waits_lets_go_of_the_body_so_its_answer_is_read() {
        // The fill suite's object, sent in pieces of a MiB.
        const OBJECT: usize = 32 << 20;
        const PIECE: usize = 1 << 20;
        /// Where the connection is in its exchange.
        enum Connection {
            /// Reading the reply for the host's word that it wants the body.
            Asking,
            /// Sending the body from this octet, its end once past the object.
            Sending(usize),
            /// Reading the reply, the body sent or its channel closed.
            Reading,
        }
        let mut host = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
        let (body, pieces) = hyper_rt::sync::channel(door::Depth::FIRST).unwrap();
        let (reply, mut answers) = hyper_rt::sync::channel(door::Replying::FIRST_ANSWERS).unwrap();
        let (mut job, _, _) =
            local_job(Work::Put(native_put(u64::try_from(OBJECT).unwrap())), 0, 0);
        job.door = Door::Local(Box::new(Local {
            body: Some(Pieces {
                pieces,
                ended: None,
                dry: false,
            }),
            reply,
            pending: VecDeque::new(),
            ready: false,
            mark: None,
        }));
        host.push(job, 0);
        // Three volumes, each refusing its copy of the block for want of room until the ask at
        // the second beat, which its cleaner answers full.
        let mut stub = Stub {
            answer: |_| Outcome::Chunk(ChunkOutcome::Refused(chunk::Status::Busy)),
            table: Some(Table::volumes(1)),
            ..Stub::chunks()
        };
        let object = Bytes::from(vec![7u8; OBJECT]);
        let mut connection = Connection::Asking;
        let (mut beats, mut answer) = (0, None);
        for _ in 0..256 {
            match connection {
                Connection::Asking => match answers.try_recv().unwrap() {
                    Some(door::Answer::Ready) => connection = Connection::Sending(0),
                    Some(other) => answer = Some(other),
                    None => {}
                },
                Connection::Sending(at) => {
                    let end = (at + PIECE).min(OBJECT);
                    let piece = if at < OBJECT {
                        let paced = door::Paced {
                            sent: std::time::Instant::now(),
                            delivered_ns: None,
                        };
                        Piece::Bytes(object.slice(at..end), paced)
                    } else {
                        Piece::End(None)
                    };
                    match body.try_send(piece) {
                        Ok(()) if at < OBJECT => connection = Connection::Sending(end),
                        // Waiting for room, it reads nothing of the reply.
                        Err(SyncError::Full(_)) => {}
                        // The body sent, or let go of by the host: its answer is read.
                        Ok(()) | Err(_) => connection = Connection::Reading,
                    }
                }
                Connection::Reading => answer = answers.try_recv().unwrap(),
            }
            if answer.is_some() {
                break;
            }
            // A beat passes while the job waits for room: its chunks are asked again, as the
            // host's step offers room at each beat (`Host::step`).
            if host.jobs.first().is_some_and(Job::waits_for_room) {
                beats += 1;
                if beats == 2 {
                    stub.answer = |_| Outcome::Chunk(ChunkOutcome::Refused(chunk::Status::Full));
                }
                host.offer_room(&mut stub, 0).unwrap();
            }
            host.step(&mut stub).unwrap();
        }
        assert_eq!(beats, 2, "held for room past two beats");
        let Some(door::Answer::Head(reply)) = answer else {
            panic!(
                "the connection waits for room in the body's channel, which the job no longer \
                 takes, and the job for room in the reply's, which the connection reads only once \
                 the body is sent: {answer:?}"
            );
        };
        assert_eq!(reply.status, Status::InsufficientStorage);
        assert!(
            matches!(connection, Connection::Reading),
            "the body's rest unsent"
        );
        // The host's turn the connection's take of the answer woke: the reply's end, held for
        // room, goes, and the job ends.
        host.step(&mut stub).unwrap();
        assert!(host.idle(), "the job ended once its answer was read");
        assert_eq!(answers.try_recv().unwrap(), Some(door::Answer::End));
    }

    /// Every outcome that says no room now, at the volume or in the transport toward it, here or at
    /// its peer (vendor/hyper-transport/src/error.rs).
    fn room_refusals() -> Vec<ChunkOutcome> {
        let transport = [
            Refusal::Budget,
            Refusal::Exchanges,
            Refusal::LaneFull,
            Refusal::Lanes,
            Refusal::Events,
            Refusal::ReceiveBuffer,
            Refusal::Pending,
            Refusal::Identities,
            Refusal::Connections,
            Refusal::Busy,
            Refusal::Unsent,
            Refusal::Stalled,
        ];
        std::iter::once(ChunkOutcome::Refused(chunk::Status::Busy))
            .chain(transport.map(ChunkOutcome::Lost))
            .collect()
    }

    /// A chunk the transport refused for want of room, as one its volume refused `Busy`, waits for
    /// room and is asked of the same volume again, its put neither sent on nor failed: in a cell of
    /// three, whose block's chunks take every volume, the transport's backpressure sent it to the
    /// next volume offered, of which there was none, and failed the put `Unplaced`, `503 SlowDown`
    /// (a Mac Studio's cell of three at 2,048 clients). A stalled exchange may have reached its
    /// volume, which answers the same key and bytes put again as done, so it too is asked again
    /// of the same volume, with the same key.
    #[test]
    fn a_chunk_refused_for_room_waits_and_is_asked_of_its_volume_again() {
        for outcome in room_refusals() {
            let (mut put, chunks) = put_of_copies(3);
            let (id, volume, key) = chunks[0];
            let answered = put.answer(id, put_answer(Outcome::Chunk(outcome.clone())));
            assert_eq!(answered, Ok(()), "{outcome:?}");
            assert!(put.outcome().is_none(), "{outcome:?} ended the put");
            assert!(put.waits_for_room(), "{outcome:?}");
            assert!(put.poll().is_none(), "{outcome:?} sent the chunk on");
            put.room().unwrap();
            match put.poll() {
                Some((
                    _,
                    put::Request::Chunk {
                        volume: v, key: k, ..
                    },
                )) => {
                    assert_eq!((v, k), (volume, key), "{outcome:?}");
                }
                other => panic!("{outcome:?}: {other:?}"),
            }
            assert!(!put.waits_for_room(), "{outcome:?}");
        }
    }

    /// A volume that cannot take the chunk (fenced, closing, not on the node asked, its records
    /// damaged, the bytes changed on the way, its node unreachable or its connection closed) and a
    /// full one still send it on to the next volume offered, and a block left none still fails
    /// `Unplaced` or `Full`; a chunk no volume takes as it was sent (this node's role may not send
    /// it, past its class's frame bound, its exchange out of order, the node's exchange identifiers
    /// spent, its settings invalid, or its asks crossed) fails the put at once.
    #[test]
    fn a_full_or_refusing_volume_sends_a_chunk_on_and_an_unsendable_one_fails_the_put() {
        let refusing = [
            ChunkOutcome::Refused(chunk::Status::Fenced),
            ChunkOutcome::Refused(chunk::Status::Closed),
            ChunkOutcome::Refused(chunk::Status::NoVolume),
            ChunkOutcome::Refused(chunk::Status::Corrupt),
            ChunkOutcome::Refused(chunk::Status::Checksum),
            ChunkOutcome::Refused(chunk::Status::Failed),
            ChunkOutcome::Lost(Refusal::NotConnected),
            ChunkOutcome::Lost(Refusal::Closed),
            ChunkOutcome::Lost(Refusal::Corrupt),
            ChunkOutcome::Lost(Refusal::Unknown),
            ChunkOutcome::Refused(chunk::Status::Full),
        ];
        for outcome in refusing {
            let full = outcome == ChunkOutcome::Refused(chunk::Status::Full);
            let (mut put, chunks) = put_of_copies(4);
            let (id, volume, key) = chunks[0];
            put.answer(id, put_answer(Outcome::Chunk(outcome.clone())))
                .unwrap();
            assert!(!put.waits_for_room(), "{outcome:?}");
            match put.poll() {
                Some((
                    _,
                    put::Request::Chunk {
                        volume: v, key: k, ..
                    },
                )) => {
                    assert_eq!((v, k), (3, key), "{outcome:?}");
                    assert_ne!(v, volume);
                }
                other => panic!("{outcome:?}: {other:?}"),
            }
            let (mut put, chunks) = put_of_copies(3);
            let failed = put.answer(chunks[0].0, put_answer(Outcome::Chunk(outcome.clone())));
            if full {
                assert!(matches!(failed, Err(PutError::Full(_))), "{failed:?}");
            } else {
                assert!(
                    matches!(failed, Err(PutError::Unplaced(_))),
                    "{outcome:?}: {failed:?}"
                );
            }
        }
        let unsendable = [
            ChunkOutcome::Lost(Refusal::Kind),
            ChunkOutcome::Lost(Refusal::FrameBound),
            ChunkOutcome::Lost(Refusal::Order),
            ChunkOutcome::Lost(Refusal::Exhausted),
            ChunkOutcome::Lost(Refusal::Configuration),
            ChunkOutcome::Read(Vec::new()),
            ChunkOutcome::Refused(chunk::Status::Stored),
        ];
        for outcome in unsendable {
            let (mut put, chunks) = put_of_copies(4);
            let failed = put.answer(chunks[0].0, put_answer(Outcome::Chunk(outcome.clone())));
            assert!(
                matches!(failed, Err(PutError::Unsendable(_))),
                "{outcome:?}: {failed:?}"
            );
            assert!(put.poll().is_none(), "{outcome:?} sent the chunk on");
        }
    }

    /// A put whose chunk is refused for want of room at each beat still ends at its client's
    /// patience, its refusals no movement of its work, and `bound_jobs` answers it `SlowDown` for
    /// its patience, not `Unplaced`; an ask answered otherwise is its work moving. Counted as
    /// movement, each refusal began its patience again, and a put whose room never came waited on.
    #[test]
    fn a_put_refused_for_room_at_each_beat_ends_at_its_clients_patience() {
        let patience = u64::try_from(bounds::STOCK_PATIENCE.as_nanos()).unwrap();
        let (mut put, chunks) = put_of_copies(3);
        put.answer(chunks[0].0, put::Answer::Busy).unwrap();
        let mut job = put_job(put);
        for outcome in room_refusals() {
            job.answered(&Outcome::Chunk(outcome.clone()), patience);
            assert_eq!(job.bounded(patience), Bounded::Held, "{outcome:?}");
            assert_eq!(job.bounded(patience + 1), Bounded::SlowDown, "{outcome:?}");
        }
        assert!(job.held() && job.waits_for_room());
        job.answered(&Outcome::Chunk(ChunkOutcome::Stored), patience);
        assert_eq!(job.bounded(patience + 1), Bounded::Held);
    }

    /// A keyring that gives any file a key of its own: these gets read no bytes.
    struct AnyKey;

    impl Keyring for AnyKey {
        fn unwrap(&self, _: &WrappedKey) -> Result<DataKey, GetError> {
            DataKey::generate().map_err(|_| GetError::Key)
        }
    }

    /// A get of a one-byte object in one block of three copies on volumes 1, 2 and 3 (block 9),
    /// read as far as its chunk read: the driver, and the read's ID and request.
    fn get_of_copies() -> (Get, u64, get::Request) {
        let length = mantle_s3::seal::sealed_len(1).unwrap();
        let mut get = Get::new(Some(1), 1, 0..1, Box::new(AnyKey), 1).unwrap();
        let header = record::FileHeader {
            length,
            extents: 1,
            made_ns: 0,
            deadline_ns: 0,
            referrer: record::Referrer {
                bucket: "b".into(),
                incarnation: 1,
                key: "k".into(),
            },
            key: Some(WrappedKey {
                by: Wrapper::Root(1),
                bytes: [0; 40],
            }),
        };
        let block = record::BlockHeader {
            length,
            data: 1,
            parity: 2,
            chunk_len: length,
            crc32c: 0,
        };
        let places = (1..=3)
            .map(|volume| record::ChunkPlace {
                volume,
                key: mantle_chunk::ChunkKey {
                    block: 9,
                    epoch: 1,
                    index: 0,
                },
            })
            .collect();
        let extent = record::Extent {
            length,
            target: record::Target::Block(9),
        };
        for answer in [
            get::Answer::Header(Some(header)),
            get::Answer::Extents(vec![(0, extent)]),
            get::Answer::Block(Some((block, places))),
        ] {
            let (id, _) = get.poll().unwrap();
            get.answer(id, answer).unwrap();
        }
        let (id, request) = get.poll().unwrap();
        assert!(
            get.poll().is_none(),
            "a copy is read from one place at a time"
        );
        (get, id, request)
    }

    /// A chunk read refused for want of room, at the volume's reads or in the transport toward
    /// it, here or at its peer, or stalled a period at a peer too busy to move it, leaves its get
    /// waiting, and once room frees asks the same copy for the same bytes again: it is no copy
    /// lost and no failed read. Every such refusal was `Unreadable`, another copy read in its
    /// stead, and a cell of three whose copies were all refused for room answered the GET
    /// `503 SlowDown`, "fewer of a block's chunks could be read than rebuild it".
    #[test]
    fn a_chunk_read_refused_for_room_waits_and_is_asked_of_its_copy_again() {
        for outcome in room_refusals() {
            let (mut get, id, request) = get_of_copies();
            let answered = get.answer(id, get_answer(Outcome::Chunk(outcome.clone())));
            assert_eq!(answered, Ok(()), "{outcome:?}");
            assert!(get.outcome().is_none(), "{outcome:?} ended the get");
            assert!(get.waits_for_room(), "{outcome:?}");
            assert!(get.poll().is_none(), "{outcome:?} read another copy");
            get.room().unwrap();
            let (_, again) = get.poll().unwrap();
            assert_eq!(again, request, "{outcome:?}");
            assert!(!get.waits_for_room(), "{outcome:?}");
        }
    }

    /// A copy that cannot be read (its volume fenced, closing or not on the node asked, the chunk
    /// not found, past its end or corrupt, a reply that is no read's, its peer gone or not the one
    /// named) is read from another place; a read no volume serves as it was asked (this node's
    /// role, its frame bound, the exchange's order, identifiers or settings, or a put's outcome
    /// answering it) ends the get `Unsendable`, `500`, as reading another place would be refused
    /// alike; a read's bytes are its answer.
    #[test]
    fn a_get_reads_another_copy_only_where_this_one_cannot_be_read() {
        let elsewhere = [
            ChunkOutcome::Refused(chunk::Status::Fenced),
            ChunkOutcome::Refused(chunk::Status::Closed),
            ChunkOutcome::Refused(chunk::Status::NoVolume),
            ChunkOutcome::Refused(chunk::Status::NotFound),
            ChunkOutcome::Refused(chunk::Status::Range),
            ChunkOutcome::Refused(chunk::Status::Corrupt),
            ChunkOutcome::Refused(chunk::Status::Failed),
            ChunkOutcome::Refused(chunk::Status::Malformed),
            ChunkOutcome::Refused(chunk::Status::Full),
            ChunkOutcome::Refused(chunk::Status::Stored),
            ChunkOutcome::Lost(Refusal::Closed),
            ChunkOutcome::Lost(Refusal::NotConnected),
            ChunkOutcome::Lost(Refusal::Identity),
            ChunkOutcome::Lost(Refusal::UnknownExchange),
            ChunkOutcome::Lost(Refusal::Corrupt),
            ChunkOutcome::Lost(Refusal::Quic),
            ChunkOutcome::Lost(Refusal::Unknown),
            ChunkOutcome::Suspected,
        ];
        for outcome in elsewhere {
            let (mut get, id, request) = get_of_copies();
            get.answer(id, get_answer(Outcome::Chunk(outcome.clone())))
                .unwrap();
            assert!(!get.waits_for_room(), "{outcome:?}");
            let (_, next) = get.poll().unwrap();
            let (get::Request::Chunk { volume: was, .. }, get::Request::Chunk { volume, .. }) =
                (request, next)
            else {
                panic!("{outcome:?}");
            };
            assert_eq!((was, volume), (1, 2), "{outcome:?}");
        }
        let unsendable = [
            ChunkOutcome::Lost(Refusal::Kind),
            ChunkOutcome::Lost(Refusal::FrameBound),
            ChunkOutcome::Lost(Refusal::Order),
            ChunkOutcome::Lost(Refusal::Exhausted),
            ChunkOutcome::Lost(Refusal::Configuration),
            ChunkOutcome::Stored,
        ];
        for outcome in unsendable {
            let (mut get, id, _) = get_of_copies();
            let failed = get.answer(id, get_answer(Outcome::Chunk(outcome.clone())));
            assert_eq!(failed, Err(GetError::Unsendable(9)), "{outcome:?}");
            assert!(get.poll().is_none(), "{outcome:?}");
            assert_eq!(get_failed(&GetError::Unsendable(9)).status, Status::Failed);
        }
        assert_eq!(
            get_answer(Outcome::Chunk(ChunkOutcome::Read(vec![7]))),
            get::Answer::Chunk(Bytes::from(vec![7]))
        );
    }

    /// A copy whose place holds no such chunk, or whose node holds no such volume, is read from
    /// another place, and where none gives it, from the block's row read again, once: repair may
    /// have moved it after the get read the row, and the volume it left, drained, been retired
    /// or its copy deleted (repair.md §3–4). Answered unreadable, the get ended `Unreadable`,
    /// `503 SlowDown`, the copy safe on another volume. A chunk that fails its checksum at every
    /// place is a corruption, which its volume reports for repair, and no stale row: no row is
    /// read again, and the get ends `Unreadable` as before.
    #[test]
    fn a_get_reads_its_blocks_row_again_only_where_a_place_holds_no_such_chunk() {
        for absent in [chunk::Status::NotFound, chunk::Status::NoVolume] {
            let answer = || get_answer(Outcome::Chunk(ChunkOutcome::Refused(absent)));
            assert_eq!(answer(), get::Answer::Absent);
            let (mut get, mut id, _) = get_of_copies();
            for volume in [2, 3] {
                get.answer(id, answer()).unwrap();
                let (next, request) = get.poll().unwrap();
                assert!(
                    matches!(request, get::Request::Chunk { volume: v, .. } if v == volume),
                    "{absent:?}: {request:?}"
                );
                id = next;
            }
            get.answer(id, answer()).unwrap();
            let (_, request) = get.poll().unwrap();
            assert_eq!(request, get::Request::Block { block: 9 }, "{absent:?}");
            assert_eq!(get.outcome(), None, "{absent:?}");
        }
        let corrupt = || {
            get_answer(Outcome::Chunk(ChunkOutcome::Refused(
                chunk::Status::Corrupt,
            )))
        };
        let (mut get, mut id, _) = get_of_copies();
        for _ in 0..2 {
            get.answer(id, corrupt()).unwrap();
            id = get.poll().unwrap().0;
        }
        assert_eq!(get.answer(id, corrupt()), Err(GetError::Unreadable(9)));
        assert!(get.poll().is_none());
    }

    /// A copy whose node the detectors suspect is read after every other place, and, every other
    /// unreadable, asked again past the suspicion, once, its bytes then the get's: a detector's
    /// mistake, which a loaded host makes, never fails the read of a block a node still up holds.
    /// Answered lost at once, a block whose only copy a suspected founder held ended
    /// `Unreadable`, `503 SlowDown`, read through the node that had just joined its cell.
    #[test]
    fn a_suspected_copy_is_read_past_the_suspicion_once_no_other_reads() {
        let (mut get, id, first) = get_of_copies();
        get.answer(id, get_answer(Outcome::Chunk(ChunkOutcome::Suspected)))
            .unwrap();
        let mut asked = vec![first];
        let mut past = Vec::new();
        while let Some((id, request)) = get.poll() {
            let get::Request::Chunk {
                volume,
                past_suspicion,
                ..
            } = request
            else {
                panic!("{request:?}");
            };
            asked.push(request.clone());
            if past_suspicion {
                past.push(volume);
                // The place answers past the suspicion: its bytes end the read.
                break;
            }
            // Fenced, unreadable without being absent: a place absent has the row read again
            // first (`a_get_reads_its_blocks_row_again_only_where_a_place_holds_no_such_chunk`).
            get.answer(
                id,
                get_answer(Outcome::Chunk(ChunkOutcome::Refused(chunk::Status::Fenced))),
            )
            .unwrap();
        }
        let volumes: Vec<u128> = asked
            .iter()
            .filter_map(|r| match r {
                get::Request::Chunk { volume, .. } => Some(*volume),
                _ => None,
            })
            .collect();
        assert_eq!(volumes, vec![1, 2, 3, 1], "{asked:?}");
        assert_eq!(past, vec![1]);
        assert_eq!(get.outcome(), None);
    }

    /// A get whose chunk read waits for room is held, due at its beat on either door so the node
    /// offers it room again, and its refusals for room are no movement of its work: refused at
    /// each beat, it still ends at its client's patience, `SlowDown` for patience while its reply
    /// is held. Once its reply began, past that patience it is vouched for no more, and its driver
    /// ends `Patience`, which no transport would end for it, so its response ends unfinished.
    #[test]
    fn a_get_refused_for_room_at_each_beat_ends_at_its_clients_patience() {
        let patience = u64::try_from(bounds::STOCK_PATIENCE.as_nanos()).unwrap();
        let (mut get, id, _) = get_of_copies();
        get.answer(id, get::Answer::Busy).unwrap();
        let work = getting("b".into(), "k".into(), Headers::default(), None, false);
        let (mut job, _body, _answers) = local_job(work, 1, 0);
        let Work::Get(getting) = &mut job.work else {
            panic!("the job is a get");
        };
        getting.get = Some(get);
        getting.held = Some(Box::new(Reply::status(Status::Done)));
        assert!(job.held() && job.waits_for_room());
        assert_eq!(job.due_ns(5, 2), Some(7), "the S3 door's get, at its beat");
        for outcome in room_refusals() {
            job.answered(&Outcome::Chunk(outcome.clone()), patience);
            assert_eq!(job.bounded(patience), Bounded::Held, "{outcome:?}");
            assert_eq!(job.bounded(patience + 1), Bounded::SlowDown, "{outcome:?}");
        }
        assert_eq!(
            get_failed(&GetError::Patience).slowed,
            Some(door::Slowed::Patience)
        );
        let Work::Get(getting) = &mut job.work else {
            panic!("the job is a get");
        };
        getting.held = None;
        assert_eq!(job.bounded(patience + 1), Bounded::Unvouched);
        assert!(
            job.waits_for_room(),
            "past its patience, nothing else ends it"
        );
        let Work::Get(getting) = &mut job.work else {
            panic!("the job is a get");
        };
        let get = getting.get.as_mut().unwrap();
        get.out_of_patience();
        assert_eq!(get.outcome(), Some(&Err(GetError::Patience)));
        assert!(!job.waits_for_room() && !job.held());
    }

    /// A native reply's head goes at its first offer, as it fills the room the endpoint took with
    /// its request's head (`node::reply_head`), a get's as a status's. What still waits for room is
    /// the piece its request's body drains through (`Node::drop_body`'s `Room`): the reply is held
    /// and vouched for within its client's patience, its node's turn due at the beat, and goes
    /// whole once the body is read; past its patience its exchange is refused `Budget`, which its
    /// asker hears at once. Taken as the reply's end, as the job's drain once took it, its exchange
    /// stayed open with nothing more to come, and its client gave it up a period later
    /// (`Lost(Stalled)`, tests/put_body.rs).
    #[test]
    fn a_reply_whose_body_waits_for_room_is_held_then_goes_or_is_refused() {
        let (head, fields) = Reply::status(Status::Done).encode().unwrap();
        let opening = |bound_ns| Opening {
            head: head.clone(),
            fields: Bytes::from(fields.clone()),
            body: 0,
            slowed: None,
            bound_ns,
            admitted: false,
        };
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        let (mut native, mut replies) = native_door().unwrap();
        assert_eq!(
            native.offer(Told::Reply(Box::new(opening(100)))),
            Sent::Taken
        );
        let mut seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        assert_eq!(
            replies.write(&mut seen, false, 5, &mut moved, &mut slowed),
            Wrote::Draining
        );
        // The piece its body drains through refused for room (`Node::write_reply`): vouched for
        // at its first turn waiting and half a beat (3 of a beat of 7) after its last.
        for (now, told) in [(10, 1), (12, 1), (20, 2), (98, 3)] {
            assert_eq!(replies.wait_for_room(&mut seen, now, 7), Wrote::Room);
            assert_eq!(
                seen.working, told,
                "vouched for half a beat apart, at {now}"
            );
            let vouched = if now == 12 { 10 } else { now };
            assert_eq!(
                replies.room_due_ns(7),
                Some((vouched + 3).min(100)),
                "due at its next vouch, at most its patience"
            );
        }
        assert_eq!(
            replies.write(&mut seen, true, 99, &mut moved, &mut slowed),
            Wrote::Over
        );
        assert_eq!(seen.answered, Some((head.clone(), fields.len() as u64)));
        assert_eq!(seen.body, fields);
        assert!(seen.ended && !seen.refused);
        assert_eq!(replies.room_due_ns(7), None);

        // Past its patience: refused `Budget`, nothing written.
        let (mut native, mut replies) = native_door().unwrap();
        assert_eq!(
            native.offer(Told::Reply(Box::new(opening(100)))),
            Sent::Taken
        );
        let mut seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        assert_eq!(
            replies.write(&mut seen, false, 50, &mut moved, &mut slowed),
            Wrote::Draining
        );
        assert_eq!(replies.wait_for_room(&mut seen, 50, 7), Wrote::Room);
        assert_eq!(replies.wait_for_room(&mut seen, 100, 7), Wrote::Over);
        assert_eq!(seen.refusal, Some(Refusal::Budget));
        assert!(seen.answered.is_none() && !seen.ended);

        // A get's head goes at its first offer, its fields after it, and nothing waits.
        let (mut native, mut replies) = native_door().unwrap();
        let mut get = opening(100);
        get.body = 3;
        assert_eq!(native.give(Told::Head(Box::new(get))), Sent::Taken);
        let mut seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        assert_eq!(
            replies.write(&mut seen, false, 10, &mut moved, &mut slowed),
            Wrote::More
        );
        assert_eq!(seen.answered, Some((head.clone(), fields.len() as u64 + 3)));
        assert_eq!(seen.body, fields);
        assert_eq!(replies.room_due_ns(7), None);

        // A reply no message holds refuses its exchange, so its asker hears it at once.
        let (mut native, mut replies) = native_door().unwrap();
        assert_eq!(native.offer(Told::Failed(Refusal::FrameBound)), Sent::Taken);
        let mut seen = Seen::default();
        assert_eq!(
            replies.write(&mut seen, true, 0, &mut moved, &mut slowed),
            Wrote::Over
        );
        assert_eq!(seen.refusal, Some(Refusal::FrameBound));
    }

    /// Replies whose request's body waits for room in the endpoint's budget, idle and offered room
    /// at each of sixteen turns a beat (`Node::write_reply`): each turn told every one's asker its
    /// work is held, one call into the transport each per turn; now each at its first turn waiting
    /// and then half a beat after its last (`Replies::wait_for_room`), the node's turn due by its
    /// next vouch (`Replies::room_due_ns`).
    #[test]
    fn a_reply_waiting_for_room_is_vouched_for_twice_a_beat_not_at_each_turn() {
        const WAITING: usize = 16;
        const BEATS: u64 = 3;
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap();
        let every = vouch_every(beat);
        let (head, fields) = Reply::status(Status::Done).encode().unwrap();
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        let patience = 10 * beat;
        let mut waiting: Vec<(Native, Replies, Seen)> = (0..WAITING)
            .map(|_| {
                let (mut native, mut replies) = native_door().unwrap();
                let opening = Opening {
                    head: head.clone(),
                    fields: Bytes::from(fields.clone()),
                    body: 0,
                    slowed: None,
                    bound_ns: patience,
                    admitted: false,
                };
                assert_eq!(native.offer(Told::Reply(Box::new(opening))), Sent::Taken);
                let mut seen = Seen {
                    credit: usize::MAX,
                    ..Seen::default()
                };
                assert_eq!(
                    replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
                    Wrote::Draining
                );
                (native, replies, seen)
            })
            .collect();
        let mut per_turn = Vec::new();
        let mut last = vec![None; WAITING];
        let mut now = 1;
        while now <= 1 + BEATS * beat {
            let mut told = 0;
            for (at, (_, replies, seen)) in waiting.iter_mut().enumerate() {
                let before = seen.working;
                assert_eq!(replies.wait_for_room(seen, now, beat), Wrote::Room);
                if seen.working > before {
                    if let Some(at) = last[at] {
                        assert!(now - at <= every, "vouched {} ns after", now - at);
                    }
                    last[at] = Some(now);
                    told += seen.working - before;
                }
                // Its node's turn is due by its next vouch, within half a beat.
                let due = replies.room_due_ns(beat).unwrap();
                assert!(due > now && due <= now + every, "due {due} at {now}");
            }
            per_turn.push(told);
            now += beat / 16;
        }
        let turns = per_turn.len();
        let told: usize = per_turn.iter().sum();
        println!(
            "{WAITING} waiting for room over {turns} turns: {told} vouches, {} before",
            WAITING * turns
        );
        assert!(turns > 16 * usize::try_from(BEATS).unwrap());
        // Two a beat each, the first turn's and each half beat's after.
        assert_eq!(told, WAITING * (2 * usize::try_from(BEATS).unwrap() + 1));
        assert_eq!(per_turn[0], WAITING, "each at its first turn waiting");
    }

    /// A reply waiting for room in the endpoint's budget is offered it again at the turn the budget
    /// gives bytes back (`Writes::wake_rooms`, `Node::host_due`), and visited at no turn while
    /// nothing it waits on changed: 64 replies in flight, 16 of them refused a piece at a budget
    /// holding `SHORT` bytes, each vouched for as it was, then turns a sixteenth of a beat apart.
    /// Before, each waiting reply was marked again at every turn, and its node's turn was due only
    /// at its next vouch, half a beat after its last: room given back just after a vouch was
    /// offered half a beat (512 ms) later.
    #[test]
    fn a_reply_waiting_for_room_is_offered_it_as_the_budget_gives_bytes_back() {
        const IN_FLIGHT: u64 = 64;
        const WAITING: u64 = 16;
        const SHORT: u64 = 1 << 20;
        let beat = room_beat();
        let every = vouch_every(beat);
        let mut writes: Writes<u64> = Writes::default();
        // Each door's job half kept, so no reply's job ends while it is counted.
        let mut halves = Vec::new();
        for exchange in 0..IN_FLIGHT {
            let (native, replies) = native_door().unwrap();
            assert!(writes.push((exchange, replies)).is_none());
            halves.push(native);
        }
        let visit = |writes: &mut Writes<u64>| {
            writes.begin_visits();
            let mut visited = 0u64;
            while writes.next_visit().is_some() {
                visited += 1;
            }
            visited
        };
        assert_eq!(
            visit(&mut writes),
            IN_FLIGHT,
            "each visited as it was pushed"
        );
        // Refused at 0, the budget holding `SHORT` bytes: each vouched for then, due half a beat
        // on.
        for exchange in 0..WAITING {
            writes.change(exchange, |replies| replies.room = Some((10 * beat, 0)));
        }
        writes.short_of(SHORT);
        assert_eq!(writes.first(), Some(every));
        // Turns with the budget as it was, or holding more: none of them visited.
        let (mut visited, mut turns) = (0, 0);
        let mut now = 1;
        while now < every {
            let used = SHORT + (now % 3) * 4096;
            assert!(!writes.room_freed(used), "no turn due for room at {now}");
            assert_eq!(writes.wake_rooms(used, now), 0);
            visited += visit(&mut writes);
            turns += 1;
            now += beat / 16;
        }
        assert_eq!(visited, 0, "nothing visited while nothing changed");
        // Bytes given back at `now`: the node's turn is due at once, and every waiting reply is
        // offered room at it, where its next vouch was `every`.
        let freed_at = every / 2 + 1;
        let used = SHORT - 1;
        assert!(writes.room_freed(used), "the node turns at once");
        let before = writes.first().unwrap() - freed_at;
        assert_eq!(
            writes.wake_rooms(used, freed_at),
            usize::try_from(WAITING).unwrap()
        );
        assert_eq!(
            visit(&mut writes),
            WAITING,
            "each offered room at that turn"
        );
        println!(
            "{WAITING} of {IN_FLIGHT} replies waiting for room over {turns} turns: 0 visits, {} \
             before; room given back {before} ns before their next vouch, offered it at once",
            WAITING * turns
        );
        assert!(before >= every / 2 - 1);
        // Offered once: until a visit finds the budget short again, no wait is cut short, so a
        // reply whose visit drained what came and now waits for its body turns no node.
        assert!(!writes.room_freed(0));
        assert_eq!(writes.wake_rooms(0, freed_at + 1), 0);
        // Short again at fewer bytes, its next vouch comes: each visited then, by its due.
        writes.short_of(used);
        assert!(!writes.room_freed(used));
        assert_eq!(
            writes.wake_rooms(used, every),
            usize::try_from(WAITING).unwrap()
        );
        assert_eq!(visit(&mut writes), WAITING);
        writes.audit();
        drop(halves);
    }

    /// A get's piece its client's room took only in part is timed from then: once written whole,
    /// what its client took since its room was full, over the time since, is its pace for the
    /// windows the node's next gets hold (`Drain`, `crate::window`); a piece its room took at once
    /// says nothing, as no window held it back.
    #[test]
    fn a_native_gets_client_is_paced_from_when_its_room_was_full() {
        let (head, fields) = Reply::status(Status::Done).encode().unwrap();
        let opening = Opening {
            head,
            fields: Bytes::from(fields),
            body: 10,
            slowed: None,
            bound_ns: 100,
            admitted: true,
        };
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        let (mut native, mut replies) = native_door().unwrap();
        let mut seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        assert_eq!(native.give(Told::Head(Box::new(opening))), Sent::Taken);
        assert_eq!(
            replies.write(&mut seen, false, 0, &mut moved, &mut slowed),
            Wrote::More
        );
        // Taken at once: no pace.
        assert_eq!(
            native.give(Told::Bytes(Bytes::from(vec![1; 4]))),
            Sent::Taken
        );
        assert_eq!(
            replies.write(&mut seen, false, 1, &mut moved, &mut slowed),
            Wrote::More
        );
        assert_eq!(replies.take_paced(), None);
        // Its room full after 2 of 6 bytes at 10 ns, then a byte at 20 and the last 3 at 40: 4
        // bytes in 30 ns.
        seen.credit = 2;
        assert_eq!(
            native.give(Told::Bytes(Bytes::from(vec![2; 6]))),
            Sent::Taken
        );
        for (credit, now) in [(2, 10), (1, 20)] {
            seen.credit = credit;
            assert_eq!(
                replies.write(&mut seen, false, now, &mut moved, &mut slowed),
                Wrote::Still
            );
            assert_eq!(replies.take_paced(), None);
        }
        seen.credit = 3;
        assert_eq!(
            replies.write(&mut seen, false, 40, &mut moved, &mut slowed),
            Wrote::More
        );
        assert_eq!(replies.take_paced(), Some((4, 30)));
        assert_eq!(replies.take_paced(), None);
    }

    /// Every answer a door's reply channel holds, in order.
    fn answered(answers: &mut ChannelReceiver<door::Answer>) -> Vec<door::Answer> {
        let mut taken = Vec::new();
        while let Ok(Some(answer)) = answers.try_recv() {
            taken.push(answer);
        }
        taken
    }

    /// A host driven only through its interface (node.md §2.9 step 6d), by a stub of the node that
    /// answers each ask at the host's next turn, steps a put and a get to their ends: the put of an
    /// empty object takes its body's end from its door, asks its Name range to commit it, and is
    /// answered `Done` with the version the range wrote; the get reads its current version, an
    /// empty one, makes its driver over it and is answered `Done` with no bytes; and each job's end
    /// is told with its admission's charge, its shard and no ask let go.
    #[test]
    fn a_host_driven_only_through_its_interface_steps_a_put_and_a_get_to_their_ends() {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(2, Some(std::sync::Arc::new(scheming_gateway())));
        let mut putting = native_put(0);
        putting.trailing = None;
        let (mut put, body, mut put_answers) = local_job(Work::Put(putting), now, 7);
        put.driven = Some(1);
        // The put's body as the S3 door ends it: whole, its trailer's checksum the empty body's.
        let crc = checksum::checksum(Algorithm::Crc32c, &[]).unwrap();
        assert!(body.try_send(Piece::End(Some(crc))).is_ok());
        let get = work(
            Op::Get {
                bucket: "b".into(),
                key: "k".into(),
                headers: Headers::default(),
                version: None,
            },
            None,
        );
        // Admitted at what a get is, the floor's window read (`sized`): its version read, its
        // window holds no more (`reading`).
        let read = scheming_gateway().charges.read(1);
        let (get, _, mut get_answers) = local_job(get, now, read);
        host.push(put, now);
        host.push(get, now);
        let mut stub = put_and_get_stub();
        let mut turns = 0;
        while !host.idle() {
            turns += 1;
            assert!(turns <= 8, "the jobs never ended");
            host.step(&mut stub).unwrap();
        }
        assert_eq!(stub.asked, 2, "the put's commit and the get's read");
        assert!(stub.answers.is_empty() && stub.abandoned == 0);
        stub.ended.sort();
        assert_eq!(
            stub.ended,
            vec![(Some(now), 7, Some(1), 0), (Some(now), read, None, 0)]
        );
        let put_answers = answered(&mut put_answers);
        assert!(
            matches!(put_answers.as_slice(), [door::Answer::Ready, door::Answer::Head(reply), door::Answer::End]
                if reply.status == Status::Done && reply.version == "v" && reply.size == 0),
            "{put_answers:?}"
        );
        let get_answers = answered(&mut get_answers);
        assert!(
            matches!(get_answers.as_slice(), [door::Answer::Head(reply), door::Answer::End]
                if reply.status == Status::Done && reply.size == 0),
            "{get_answers:?}"
        );
    }

    /// A native request's job of `work`, handed with its fields read, so its door names no
    /// exchange (node.md §2.9 step 6e), admitted at `now`, charged `charge` and placed on shard 1;
    /// the node's task's half of its door beside it.
    fn handed_job(work: Work, now: u64, charge: u64) -> (Job, Replies) {
        let (native, replies) = native_door().unwrap();
        let job = Job {
            door: Door::Native(None, Box::new(native)),
            principal: "o".into(),
            work,
            pending: Vec::new(),
            deadline_ns: None,
            moved_ns: now,
            vouched_ns: None,
            resting: false,
            admitted_ns: Some(now),
            charge,
            window: 1,
            driven: Some(1),
            label: None,
        };
        (job, replies)
    }

    /// The reply an exchange was written: its head's, and the fields past it.
    fn reply_seen(seen: &Seen) -> Reply {
        let (head, _) = seen.answered.as_ref().unwrap();
        let head = object::ReplyHead::decode(head).unwrap();
        let fields = usize::try_from(head.fields()).unwrap();
        head.reply(&seen.body[..fields]).unwrap()
    }

    /// A stub that answers a put's commit of its Name range and a get's read of its current
    /// version, an empty one.
    fn put_and_get_stub() -> Stub {
        Stub {
            answer: |ask| match ask {
                JobAsk::Command(_, Command::Name(_)) => {
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(name::Outcome::Put {
                        version: "v".into(),
                    })))
                }
                JobAsk::Read(_, Query::Current { .. }) => Outcome::Read(ReadOutcome::Read(
                    Box::new(Read::Current(Some((1, empty_version())))),
                )),
                _ => panic!("an ask neither job makes"),
            },
            answers: Vec::new(),
            asked: 0,
            no_room: false,
            table: None,
            ended: Vec::new(),
            abandoned: 0,
            takes_halves: false,
            half: None,
            arrived: Vec::new(),
        }
    }

    /// A native request handed with its fields read names no exchange (node.md §2.9 step 6e): only
    /// hyper-transport makes one, and the job needs none. Driven only through `Serve`, its put
    /// takes its body's end from its door and is answered `Done` with the version written, and its
    /// get is answered `Done` with no bytes, each reply written by the node's task's half of its
    /// door as the exchange takes it; each job's half given back as it ends, and each door kept
    /// once its reply's half is given back too.
    #[test]
    fn a_native_request_handed_with_its_fields_read_needs_no_exchange() {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(2, Some(std::sync::Arc::new(scheming_gateway())));
        let mut putting = native_put(0);
        putting.trailing = None;
        let (put, mut put_replies) = handed_job(Work::Put(putting), now, 7);
        let crc = checksum::checksum(Algorithm::Crc32c, &[]).unwrap();
        let body = put_replies.body.as_ref().unwrap();
        assert!(body.try_send(Piece::End(Some(crc))).is_ok());
        let get = work(
            Op::Get {
                bucket: "b".into(),
                key: "k".into(),
                headers: Headers::default(),
                version: None,
            },
            None,
        );
        let read = scheming_gateway().charges.read(1);
        let (get, mut get_replies) = handed_job(get, now, read);
        assert!(put.door.exchange().is_none() && get.door.exchange().is_none());
        host.push(put, now);
        host.push(get, now);
        let mut stub = put_and_get_stub();
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        let mut put_seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        let mut get_seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        let (mut put_over, mut get_over) = (false, false);
        let mut turns = 0;
        while !(host.idle() && put_over && get_over) {
            turns += 1;
            assert!(turns <= 8, "the jobs never ended");
            host.step(&mut stub).unwrap();
            put_over = put_over
                || put_replies.write(&mut put_seen, true, now, &mut moved, &mut slowed)
                    == Wrote::Over;
            get_over = get_over
                || get_replies.write(&mut get_seen, true, now, &mut moved, &mut slowed)
                    == Wrote::Over;
        }
        assert_eq!(stub.asked, 2, "the put's commit and the get's read");
        stub.ended.sort();
        assert_eq!(
            stub.ended,
            vec![(Some(now), 7, Some(1), 0), (Some(now), read, Some(1), 0)]
        );
        let put_reply = reply_seen(&put_seen);
        assert_eq!(
            (put_reply.status, put_reply.version.as_str()),
            (Status::Done, "v")
        );
        let get_reply = reply_seen(&get_seen);
        assert_eq!((get_reply.status, get_reply.size), (Status::Done, 0));
        assert!(put_seen.ended && get_seen.ended && !put_seen.refused && !get_seen.refused);
        // Each job's half came back as it ended; each door is kept once its reply's half is.
        assert_eq!((host.doors.kept.len(), host.doors.waiting.len()), (0, 2));
        host.doors.give_replies(put_replies);
        host.doors.give_replies(get_replies);
        assert_eq!((host.doors.kept.len(), host.doors.waiting.len()), (2, 0));
    }

    /// A job's end carries its native door's half to whoever serves its host (node.md §2.9 step
    /// 6e), told first that its job let go: the node gives it back, and its host keeps it for the
    /// next request once its reply's half is back too; a driving shard's lanes take it to the
    /// node's task, which holds the other half, so that host keeps no door, and the node's task's
    /// doors keep it whole.
    #[test]
    fn a_jobs_end_carries_its_doors_half_to_whoever_serves_its_host() {
        for takes_halves in [false, true] {
            let mut host = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
            let done = Work::Draining(Box::new(Reply::status(Status::Done)));
            let (job, replies) = handed_job(done, 0, 0);
            host.push(job, 0);
            let mut stub = put_and_get_stub();
            stub.takes_halves = takes_halves;
            host.step(&mut stub).unwrap();
            assert!(host.idle() && stub.ended.len() == 1);
            assert!(
                replies
                    .handed
                    .ended
                    .load(std::sync::atomic::Ordering::Acquire),
                "told its job let go before its half went"
            );
            let mut doors = if takes_halves {
                assert!(host.doors.waiting.is_empty() && host.doors.kept.is_empty());
                let mut doors = Doors::new(1);
                doors.give_native(stub.half.take().unwrap());
                doors
            } else {
                assert!(stub.half.is_none());
                std::mem::take(&mut host.doors)
            };
            assert_eq!(doors.waiting.len(), 1);
            doors.give_replies(replies);
            assert_eq!((doors.kept.len(), doors.waiting.len()), (1, 0));
        }
    }

    /// A runtime of two shards, each with `tasks` task slots (as the shards' tests start theirs).
    pub(crate) fn two_shards(tasks: usize) -> hyper_rt::runtime::Runtime {
        shards_of(2, tasks)
    }

    /// A runtime of `shards` shards, each holding `tasks` tasks.
    pub(crate) fn shards_of(shards: u16, tasks: usize) -> hyper_rt::runtime::Runtime {
        hyper_rt::runtime::Runtime::start(&hyper_rt::runtime::RuntimeConfig {
            shards,
            tasks_per_shard: tasks,
            timers_per_shard: 1,
            interests_per_shard: hyper_rt::runtime::interests_for(tasks),
            ring_entries: 1,
            step_budget_ns: 1_000_000_000,
            timer_tick_ns: 100_000,
            batch: 1,
            pin: false,
            cores: Vec::new(),
            page_bytes: usize::try_from(hyper_rt::machine::facts::Facts::query().page.base)
                .unwrap(),
            spin_ns: 0,
            wake_tracking: None,
            latency_objective_ns: None,
        })
        .unwrap()
    }

    /// A bucket's row, active and owned by the asker of a handed request.
    fn bucket_row() -> Bucket {
        Bucket {
            owner: "o".into(),
            created_ns: 0,
            location: String::new(),
            versioning: Versioning::Unversioned,
            state: BucketState::Active,
            attempt: 0,
            progress_ns: 0,
            lock: None,
        }
    }

    /// A native request handed to a driving shard with its fields read: its operation and its
    /// door's job half, admitted now, charged `charge` and placed on shard 1.
    fn handover(half: Box<Native>, op: Op, charge: u64, now: u64) -> Box<Handover> {
        Box::new(Handover {
            half,
            principal: "o".into(),
            op,
            block: None,
            deadline_ns: None,
            admitted_ns: Some(now),
            charge,
            window: 1,
            driven: Some(1),
        })
    }

    /// A host on a driving shard, the second of a two-shard runtime, driven over its lanes by the
    /// stub node on the test's thread (node.md §2.9 step 6e), takes a put and a get handed with
    /// their operations to their ends: the shard makes their work, its asks cross to the stub and
    /// their answers back in the lane's slots, the put takes its body's end from its door and is
    /// answered `Done` with the version written, the get is answered `Done` with no bytes, each
    /// reply written by the test as the node's task writes it, telling the shard's word (6f); each
    /// job's end is told with its charge and its shard, its door's half riding it back, so the
    /// shard's host keeps no door and the test's doors keep both. The get's head is written only
    /// once the put ended, so its job waits on that word alone: a reply that tells none leaves the
    /// shard parked with the head written, and the get never ends. A request past the lane's room
    /// is given back.
    #[test]
    fn a_driving_shards_host_takes_a_put_and_a_get_to_their_ends_over_its_lanes() {
        let rt = two_shards(4);
        let mut gateway = scheming_gateway();
        gateway.keys = vec![(1, WrappingKey::generate().unwrap())];
        let gateway = std::sync::Arc::new(gateway);
        let start = super::super::driving::Start {
            command_asks: 2,
            read_asks: 2,
            chunk_asks: 2,
            groups: vec![(Layer::Bucket, 1), (Layer::Name, 2)],
            ranges: 2,
            wall: super::super::Wall::From(0),
            gateway,
            requests: 3,
        };
        let holder = rt.holder_of(rt.shard_ids()[1]).unwrap();
        let mut lanes = super::super::driving::Lanes::start(holder, 1, start).unwrap();
        lanes.tell_table(Box::new(Table::empty(1)));
        let mut doors = Doors::new(2);
        let now = hyper_rt::futures::now_ns();
        let (put_half, mut put_replies) = doors.take().unwrap();
        let (get_half, mut get_replies) = doors.take().unwrap();
        // Each reply carries the shard's word, as the node's task gives it to a request it hands
        // (`Node::read_fields`): its write tells the shard as it takes an answer or writes it whole.
        put_replies.call = Some(lanes.caller());
        get_replies.call = Some(lanes.caller());
        // The put's body as the node's task gives it: empty, whole.
        let body = put_replies.body.as_ref().unwrap();
        assert!(body.try_send(Piece::End(None)).is_ok());
        let put = Op::Put {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            length: 0,
            headers: Headers::default(),
            checksum: None,
            md5: None,
            trailing: None,
            metadata: Vec::new(),
        };
        let get = Op::Get {
            bucket: "b".into(),
            key: "k".into(),
            headers: Headers::default(),
            version: None,
        };
        let read = scheming_gateway().charges.read(1);
        assert!(lanes.hand(handover(put_half, put, 7, now)).is_none());
        assert!(lanes.hand(handover(get_half, get, read, now)).is_none());
        let mut stub = Stub {
            answer: |ask| match ask {
                JobAsk::Read(_, Query::Bucket { .. }) => Outcome::Read(ReadOutcome::Read(
                    Box::new(Read::Bucket(Some(bucket_row()))),
                )),
                JobAsk::Command(_, Command::Name(_)) => {
                    Outcome::Command(CommandOutcome::Answered(Answer::Name(name::Outcome::Put {
                        version: "v".into(),
                    })))
                }
                JobAsk::Read(_, Query::Current { .. }) => Outcome::Read(ReadOutcome::Read(
                    Box::new(Read::Current(Some((1, empty_version())))),
                )),
                _ => panic!("an ask neither job makes"),
            },
            ..Stub::chunks()
        };
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        let mut put_seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        let mut get_seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        let (mut put_over, mut get_over) = (false, false);
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !(put_over && get_over && stub.ended.len() == 2) {
            assert!(std::time::Instant::now() < until, "the jobs never ended");
            lanes
                .serve(&mut stub, |half| doors.give_native(half))
                .unwrap();
            let now = hyper_rt::futures::now_ns();
            put_over = put_over
                || put_replies.write(&mut put_seen, true, now, &mut moved, &mut slowed)
                    == Wrote::Over;
            // The get's reply is written only once the put's end is told. Its version's answer
            // came no later than the put's last (each answer comes in its ask's order), so by then
            // its job gave its head at a step past which it waits for the head to be written, its
            // own asks all answered: only the word its write tells turns the shard again.
            if stub.ended.iter().any(|&(_, charge, _, _)| charge == 7) {
                get_over = get_over
                    || get_replies.write(&mut get_seen, true, now, &mut moved, &mut slowed)
                        == Wrote::Over;
            }
            std::thread::yield_now();
        }
        stub.ended.sort();
        assert_eq!(
            stub.ended,
            vec![(Some(now), 7, Some(1), 0), (Some(now), read, Some(1), 0)]
        );
        let put_reply = reply_seen(&put_seen);
        assert_eq!(
            (put_reply.status, put_reply.version.as_str()),
            (Status::Done, "v")
        );
        let get_reply = reply_seen(&get_seen);
        assert_eq!((get_reply.status, get_reply.size), (Status::Done, 0));
        // Each job's half rode its end back: the test's doors keep both doors once the replies'
        // halves are back too.
        assert_eq!(doors.waiting.len(), 2);
        doors.give_replies(put_replies);
        doors.give_replies(get_replies);
        assert_eq!((doors.kept.len(), doors.waiting.len()), (2, 0));
        rt.shutdown().unwrap();

        // A request past the lane's room is given back, to run on the first shard.
        let (lanes, _ends) = super::super::driving::tests::unstarted(holder, 1, 3);
        let (half, _) = doors.take().unwrap();
        let op = || Op::Get {
            bucket: "b".into(),
            key: "k".into(),
            headers: Headers::default(),
            version: None,
        };
        assert!(lanes.hand(handover(half, op(), 0, now)).is_none());
        let (half, _) = doors.take().unwrap();
        let given_back = lanes.hand(handover(half, op(), 0, now));
        assert!(given_back.is_some_and(|request| request.driven == Some(1)));
    }

    /// A driving shard wakes for its own work alone (node.md §2.9 step 6e): two driving shards,
    /// the second and third of a three-shard runtime, served by the test's thread as the node's
    /// task serves them, room freed at every turn as each chunk the node's own requests asked frees
    /// it, and gets handed to the first of the two alone. Neither shard is told room was freed,
    /// none of its chunks waiting for it, so the shard handed nothing is polled only as its task
    /// starts and takes the table, however many turns pass, and the one handed the gets only for
    /// them. Told at every turn, as the node told it, each turn's word woke each shard, its task
    /// stepping an empty host and parking again: each S3 request's chunk woke every driving shard.
    #[test]
    fn a_driving_shard_wakes_for_its_own_work_alone() {
        const TURNS: usize = 64;
        const GETS: usize = 16;
        let rt = shards_of(3, 4);
        let gateway = std::sync::Arc::new(scheming_gateway());
        let start = || super::super::driving::Start {
            command_asks: 2,
            read_asks: 2,
            chunk_asks: 2,
            groups: vec![(Layer::Bucket, 1), (Layer::Name, 2)],
            ranges: 2,
            wall: super::super::Wall::From(0),
            gateway: std::sync::Arc::clone(&gateway),
            requests: 3,
        };
        let holder = |at: usize| rt.holder_of(rt.shard_ids()[at]).unwrap();
        let mut lanes = [
            super::super::driving::Lanes::start(holder(1), 1, start()).unwrap(),
            super::super::driving::Lanes::start(holder(2), 2, start()).unwrap(),
        ];
        for lanes in &mut lanes {
            lanes.tell_table(Box::new(Table::empty(1)));
        }
        let mut stub = Stub {
            answer: |ask| match ask {
                JobAsk::Read(_, Query::Bucket { .. }) => Outcome::Read(ReadOutcome::Read(
                    Box::new(Read::Bucket(Some(bucket_row()))),
                )),
                JobAsk::Read(_, Query::Current { .. }) => Outcome::Read(ReadOutcome::Read(
                    Box::new(Read::Current(Some((1, empty_version())))),
                )),
                _ => panic!("an ask a get makes not"),
            },
            ..Stub::chunks()
        };
        let mut doors = Doors::new(2);
        // A node's turn: room freed, as a chunk of its own requests answered frees it, and each
        // driving shard's lanes served.
        let turn =
            |lanes: &mut [super::super::driving::Lanes; 2], stub: &mut Stub, doors: &mut Doors| {
                for lanes in lanes.iter_mut() {
                    lanes.tell_freed();
                    lanes.serve(stub, |half| doors.give_native(half)).unwrap();
                }
            };
        // Turns with nothing for either shard, each yielding the test's thread as the node's task
        // waits between its turns.
        for _ in 0..TURNS {
            turn(&mut lanes, &mut stub, &mut doors);
            std::thread::yield_now();
        }
        // Gets handed to the first shard alone, each driven to its end, room freed at each turn.
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        for at in 0..GETS {
            let (half, mut replies) = doors.take().unwrap();
            replies.call = Some(lanes[0].caller());
            let get = Op::Get {
                bucket: "b".into(),
                key: "k".into(),
                headers: Headers::default(),
                version: None,
            };
            let now = hyper_rt::futures::now_ns();
            let read = scheming_gateway().charges.read(1);
            assert!(lanes[0].hand(handover(half, get, read, now)).is_none());
            let mut seen = Seen {
                credit: usize::MAX,
                ..Seen::default()
            };
            let mut over = false;
            while !(over && stub.ended.len() == at + 1) {
                assert!(std::time::Instant::now() < until, "the get never ended");
                turn(&mut lanes, &mut stub, &mut doors);
                let now = hyper_rt::futures::now_ns();
                over = over
                    || replies.write(&mut seen, true, now, &mut moved, &mut slowed) == Wrote::Over;
                std::thread::yield_now();
            }
            assert_eq!(reply_seen(&seen).status, Status::Done);
            doors.give_replies(replies);
        }
        let counters = rt.shutdown().unwrap();
        let (handed, idle) = (&counters[1], &counters[2]);
        println!(
            "{TURNS} turns, {GETS} gets: shard handed the gets polled {}, woken {} times from \
             other threads, waited {}; shard handed nothing polled {}, woken {}, waited {}",
            handed.polls,
            handed.wakes_foreign,
            handed.waits,
            idle.polls,
            idle.wakes_foreign,
            idle.waits
        );
        // Its launcher, its task's first poll, and the table's word.
        assert!(
            idle.polls <= 3,
            "a shard handed nothing is polled for nothing past its start: {} polls",
            idle.polls
        );
        drop(lanes);
    }

    /// A cell in memory as a test serves a driving shard's asks (node.md §2.9 step 6f): Block and
    /// File ranges that apply their commands to models as their engines would, volumes that keep
    /// each chunk whose CRC-32C holds, a Name range that keeps the version a put commits, and the
    /// reads of each, so a put's bytes read back through a get.
    struct Cell {
        blocks: mantle_meta::engine::Model,
        files: mantle_meta::engine::Model,
        applied: u64,
        chunks: std::collections::BTreeMap<(u128, mantle_chunk::ChunkKey), Bytes>,
        version: Option<Version>,
        answers: Vec<(Pending, Outcome)>,
        asked: u64,
        ended: usize,
    }

    impl Cell {
        fn new() -> Self {
            Self {
                blocks: mantle_meta::engine::Model::default(),
                files: mantle_meta::engine::Model::default(),
                applied: 0,
                chunks: std::collections::BTreeMap::new(),
                version: None,
                answers: Vec::new(),
                asked: 0,
                ended: 0,
            }
        }

        /// What the cell answers `ask`, as its ranges and volumes would.
        fn answer(&mut self, ask: JobAsk) -> Outcome {
            self.applied += 1;
            let read = |read: Read| Outcome::Read(ReadOutcome::Read(Box::new(read)));
            let answered = |answer: Answer| Outcome::Command(CommandOutcome::Answered(answer));
            match ask {
                JobAsk::Read(_, Query::Bucket { .. }) => read(Read::Bucket(Some(bucket_row()))),
                JobAsk::Read(_, Query::Current { .. }) => {
                    read(Read::Current(self.version.clone().map(|v| (1, v))))
                }
                JobAsk::Read(_, Query::Header { file }) => read(Read::Header(
                    mantle_meta::file::header(&mut self.files, file).unwrap(),
                )),
                JobAsk::Read(_, Query::Extents { file, offset, max }) => read(Read::Extents(
                    mantle_meta::file::extents(
                        &mut self.files,
                        file,
                        offset,
                        usize::try_from(max).unwrap(),
                    )
                    .unwrap(),
                )),
                JobAsk::Read(_, Query::Block { block }) => read(Read::Block(
                    mantle_meta::block::read(&mut self.blocks, block).unwrap(),
                )),
                JobAsk::Command(_, Command::Block(command)) => answered(Answer::Block(
                    mantle_meta::block::apply(&mut self.blocks, self.applied, &command).unwrap(),
                )),
                JobAsk::Command(_, Command::File(command)) => answered(Answer::File(
                    mantle_meta::file::apply(&mut self.files, self.applied, &command).unwrap(),
                )),
                JobAsk::Command(_, Command::Name(command)) => {
                    let name::Command::Put(put) = *command else {
                        panic!("a put's commit");
                    };
                    self.version = Some(put.version);
                    answered(Answer::Name(name::Outcome::Put {
                        version: "v".into(),
                    }))
                }
                JobAsk::Put {
                    volume,
                    key,
                    bytes,
                    crc32c,
                    ..
                } => {
                    assert_eq!(mantle_crc::crc32c(&bytes), crc32c);
                    self.chunks.insert((volume, key), bytes);
                    Outcome::Chunk(ChunkOutcome::Stored)
                }
                JobAsk::Get {
                    volume,
                    key,
                    span: (offset, len),
                    ..
                } => {
                    let chunk = &self.chunks[&(volume, key)];
                    let span =
                        usize::try_from(offset).unwrap()..usize::try_from(offset + len).unwrap();
                    Outcome::Chunk(ChunkOutcome::Read(chunk[span].to_vec()))
                }
                _ => panic!("an ask neither job makes"),
            }
        }
    }

    impl Serve for Cell {
        fn ask(&mut self, ask: JobAsk) -> Result<Pending, NodeError> {
            let pending = match &ask {
                JobAsk::Command(..) => Pending::Command(self.asked),
                JobAsk::Read(..) => Pending::Read(self.asked),
                JobAsk::Put { .. } | JobAsk::Get { .. } => Pending::Chunk(self.asked),
            };
            self.asked += 1;
            let outcome = self.answer(ask);
            self.answers.push((pending, outcome));
            Ok(pending)
        }

        fn outcome(&mut self, pending: Pending) -> Option<Outcome> {
            let at = self.answers.iter().position(|(p, _)| *p == pending)?;
            Some(self.answers.swap_remove(at).1)
        }

        fn answers(&self) -> impl Iterator<Item = Pending> {
            self.answers.iter().map(|(pending, _)| *pending)
        }

        fn abandon(&mut self, _: impl IntoIterator<Item = Pending>) {}

        fn table(&mut self) -> Result<&Table, NodeError> {
            Err(NodeError::Bounds("the shard holds its own table".into()))
        }

        fn group_of(&self, _: Layer) -> Option<u128> {
            Some(1)
        }

        fn ranges(&self) -> usize {
            4
        }

        fn command_room(&self) -> bool {
            true
        }

        fn room(&self, _: &JobAsk) -> bool {
            true
        }

        fn command_asks(&self) -> usize {
            4
        }

        fn wall_ns(&self) -> u64 {
            0
        }

        fn freed(&mut self) -> bool {
            false
        }

        fn moved(&self) -> u64 {
            0
        }

        fn read_fields(&mut self, _: &mut Job) -> Result<bool, NodeError> {
            panic!("a request is handed with its fields read")
        }

        fn took(&mut self, _: door::Paced) {}

        fn committed(&mut self, _: u64) {}

        fn filled(&mut self, _: u64, _: u64) {}

        fn read(&mut self, _: u64) {}

        fn windows(&self) -> crate::window::Windows {
            crate::window::Windows::default()
        }

        fn holds(&self, _: u64) -> bool {
            true
        }

        fn recharge(&mut self, _: u64, _: u64) {}

        fn job_ended(
            &mut self,
            _: Option<u64>,
            _: u64,
            _: Option<usize>,
            _: impl IntoIterator<Item = Pending>,
            half: Option<Box<Native>>,
        ) -> Option<Box<Native>> {
            self.ended += 1;
            half
        }
    }

    impl Table {
        /// A table of three volumes in three domains, their nodes seen, read at `applied`, whose
        /// blocks take three copies.
        fn volumes(applied: u64) -> Self {
            let facts: Vec<root::Fact> = placed_in(&["a", "b", "c"])
                .into_iter()
                .map(|placed| root::Fact {
                    placed,
                    standing: mantle_meta::record::VolumeStanding::Active,
                    presence: mantle_meta::record::Presence {
                        present: true,
                        ..mantle_meta::record::Presence::default()
                    },
                })
                .collect();
            Self {
                placed: facts.iter().map(|f| f.placed.clone()).collect(),
                standing: facts.iter().map(|f| f.placed.clone()).collect(),
                facts,
                ..Self::empty(applied)
            }
        }
    }

    /// A put of a body of many pieces and a get of its bytes in many pieces, each handed to a
    /// driving shard, the second of a two-shard runtime, and driven over its lanes by a cell in
    /// memory on the test's thread, which stands in for the node's task (node.md §2.9 step 6f):
    /// the test gives the put's body a piece at a time as the node's task feeds it, each only once
    /// the job let go of the last, and writes each reply as the node's task writes it, a get's
    /// piece taken only once the last is written whole. The shard's task waits on its lanes and
    /// its own deadlines, so each piece given or written wakes it through the door: the put is
    /// answered `Done` and the get `Done` with the object's bytes exact.
    #[test]
    fn a_driving_shards_jobs_are_woken_by_their_doors() {
        let rt = two_shards(4);
        let mut gateway = scheming_gateway();
        gateway.keys = vec![(1, WrappingKey::generate().unwrap())];
        // Held far past the test, so no block's hold lapses under it.
        gateway.handover_ns = 600_000_000_000;
        let start = super::super::driving::Start {
            command_asks: 4,
            read_asks: 4,
            chunk_asks: 4,
            groups: vec![
                (Layer::Bucket, 1),
                (Layer::Name, 2),
                (Layer::File, 3),
                (Layer::Block, 4),
            ],
            ranges: 4,
            wall: super::super::Wall::From(0),
            gateway: std::sync::Arc::new(gateway),
            requests: 3,
        };
        let holder = rt.holder_of(rt.shard_ids()[1]).unwrap();
        let mut lanes = super::super::driving::Lanes::start(holder, 1, start).unwrap();
        lanes.tell_table(Box::new(Table::volumes(1)));
        let mut doors = Doors::new(2);
        let now = hyper_rt::futures::now_ns();
        // Two blocks and part of a third: a body of many pieces, read back a block at a time.
        let segments = Layout::new(durability::Scheme::Copies(3))
            .unwrap()
            .segments_per_block();
        let block = usize::try_from(segments).unwrap() * mantle_s3::seal::SEGMENT;
        let (object, trailer) = native_body(2 * block + piece_len() / 2);
        let stream: Vec<u8> = object.iter().chain(&trailer).copied().collect();
        let put = Op::Put {
            bucket: "b".into(),
            key: "k".into(),
            id: 1,
            length: object.len() as u64,
            headers: Headers::default(),
            checksum: None,
            md5: None,
            trailing: Some(Algorithm::Crc32c),
            metadata: Vec::new(),
        };
        // The put's body as the node's task feeds it, from the sender its door lends, its door
        // carrying the shard's word as `Node::read_fields` gives it.
        let (put_half, mut put_replies) = doors.take().unwrap();
        put_replies.call = Some(lanes.caller());
        let pieces = put_replies.body.take().unwrap();
        let handed = std::sync::Arc::clone(&put_replies.handed);
        let driving = put_replies.call.clone();
        let mut feeding = Some(feed(body_of(&put).unwrap(), pieces, handed, driving));
        assert!(lanes.hand(handover(put_half, put, 7, now)).is_none());
        let mut cell = Cell::new();
        let mut gave = super::super::driving::tests::Gave::default();
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        let mut put_seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        let mut get_seen = Seen {
            credit: usize::MAX,
            ..Seen::default()
        };
        let mut rest = &stream[..];
        let mut get_replies = None;
        let (mut put_over, mut get_over) = (false, false);
        let until = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !(put_over && get_over && cell.ended == 2) {
            assert!(
                std::time::Instant::now() < until,
                "the jobs never ended: put {put_over}, get {get_over}, {} ended",
                cell.ended
            );
            lanes
                .serve_counting(&mut cell, |half| doors.give_native(half), &mut gave)
                .unwrap();
            // The body a piece at a time, each read only once the job let go of the last.
            if let Some(fed) = feeding.as_mut() {
                let over = match fed.send_held() {
                    Some(Fed::Over) => true,
                    Some(Fed::Still) => false,
                    _ if fed.read_whole() => fed.end() == Fed::Over,
                    _ if fed.free() => {
                        let (read, left) = rest.split_at(rest.len().min(piece_len()));
                        fed.give(read, &mut moved).unwrap();
                        rest = left;
                        false
                    }
                    _ => false,
                };
                if over {
                    put_replies.body = feeding.take().map(Feeding::let_go);
                }
            }
            let now = hyper_rt::futures::now_ns();
            let body_read = feeding.is_none();
            put_over = put_over
                || put_replies.write(&mut put_seen, body_read, now, &mut moved, &mut slowed)
                    == Wrote::Over;
            // The get, once the put is answered, of the bytes it wrote.
            if put_over && get_replies.is_none() {
                let (get_half, mut replies) = doors.take().unwrap();
                replies.call = Some(lanes.caller());
                let get = Op::Get {
                    bucket: "b".into(),
                    key: "k".into(),
                    headers: Headers::default(),
                    version: None,
                };
                let read = scheming_gateway().charges.read(1);
                assert!(lanes.hand(handover(get_half, get, read, now)).is_none());
                get_replies = Some(replies);
            }
            if let Some(replies) = get_replies.as_mut() {
                get_over = get_over
                    || replies.write(&mut get_seen, true, now, &mut moved, &mut slowed)
                        == Wrote::Over;
            }
            std::thread::yield_now();
        }
        // The shard's jobs' bytes, the put's body taken and the get's given, reach the node's
        // side of the lanes, summed into the node's count (`Node::moved`).
        while lanes.moved() != 2 * object.len() as u64 {
            assert!(
                std::time::Instant::now() < until,
                "the shard's count never came: {}",
                lanes.moved()
            );
            lanes
                .serve_counting(&mut cell, |half| doors.give_native(half), &mut gave)
                .unwrap();
            std::thread::yield_now();
        }
        let put_reply = reply_seen(&put_seen);
        assert_eq!(
            (put_reply.status, put_reply.size),
            (Status::Done, object.len() as u64)
        );
        let get_reply = reply_seen(&get_seen);
        assert_eq!(
            (get_reply.status, get_reply.size),
            (Status::Done, object.len() as u64)
        );
        let (head, _) = get_seen.answered.as_ref().unwrap();
        let fields = object::ReplyHead::decode(head).unwrap().fields();
        let fields = usize::try_from(fields).unwrap();
        assert!(
            get_seen.body[fields..] == object[..],
            "the object's bytes exact"
        );
        assert_eq!(
            get_seen.writes.len(),
            4,
            "the get's fields, then its three blocks"
        );
        // The bytes its jobs moved rode what else the shard gave, never a message of their own
        // at each piece taken or given.
        println!("a put and a get of {} bytes: {gave:?}", object.len());
        assert!(
            gave.moved <= gave.asks + gave.ends,
            "the bytes moved ride what else the shard gave: {gave:?}"
        );
        rt.shutdown().unwrap();
    }

    /// What a native put and a native get of one byte cost in crossings between the node's task
    /// and a driving shard (node.md §2.9 step 6f), counted over rounds of a put then a get of its
    /// bytes, each handed to the second shard of a two-shard runtime and driven over its lanes by a
    /// cell in memory on the test's thread: each ask crosses out and its answer back; each job's
    /// end and its samples ride out; and the bytes its jobs moved ride what else the shard gave,
    /// never a message of their own. The shard's polls, wakes from the test's thread and parks
    /// are said for each round.
    #[test]
    fn a_native_put_and_get_cross_the_lanes_as_counted() {
        const ROUNDS: usize = 8;
        let rt = two_shards(4);
        let mut gateway = scheming_gateway();
        gateway.keys = vec![(1, WrappingKey::generate().unwrap())];
        gateway.handover_ns = 600_000_000_000;
        let start = super::super::driving::Start {
            command_asks: 4,
            read_asks: 4,
            chunk_asks: 4,
            groups: vec![
                (Layer::Bucket, 1),
                (Layer::Name, 2),
                (Layer::File, 3),
                (Layer::Block, 4),
            ],
            ranges: 4,
            wall: super::super::Wall::From(0),
            gateway: std::sync::Arc::new(gateway),
            requests: 3,
        };
        let holder = rt.holder_of(rt.shard_ids()[1]).unwrap();
        let mut lanes = super::super::driving::Lanes::start(holder, 1, start).unwrap();
        lanes.tell_table(Box::new(Table::volumes(1)));
        let mut doors = Doors::new(2);
        let mut cell = Cell::new();
        let mut gave = super::super::driving::tests::Gave::default();
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        let until = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let (object, trailer) = native_body(1);
        let stream: Vec<u8> = object.iter().chain(&trailer).copied().collect();
        let mut per_put = None;
        for round in 0..ROUNDS {
            let put = Op::Put {
                bucket: "b".into(),
                key: "k".into(),
                id: 1,
                length: object.len() as u64,
                headers: Headers::default(),
                checksum: None,
                md5: None,
                trailing: Some(Algorithm::Crc32c),
                metadata: Vec::new(),
            };
            let now = hyper_rt::futures::now_ns();
            let (put_half, mut put_replies) = doors.take().unwrap();
            put_replies.call = Some(lanes.caller());
            let pieces = put_replies.body.take().unwrap();
            let handed = std::sync::Arc::clone(&put_replies.handed);
            let driving = put_replies.call.clone();
            let mut feeding = Some(feed(body_of(&put).unwrap(), pieces, handed, driving));
            assert!(lanes.hand(handover(put_half, put, 7, now)).is_none());
            let mut put_seen = Seen {
                credit: usize::MAX,
                ..Seen::default()
            };
            let mut rest = &stream[..];
            let mut put_over = false;
            while !(put_over && cell.ended == 2 * round + 1) {
                assert!(std::time::Instant::now() < until, "the put never ended");
                lanes
                    .serve_counting(&mut cell, |half| doors.give_native(half), &mut gave)
                    .unwrap();
                if let Some(fed) = feeding.as_mut() {
                    let over = match fed.send_held() {
                        Some(Fed::Over) => true,
                        Some(Fed::Still) => false,
                        _ if fed.read_whole() => fed.end() == Fed::Over,
                        _ if fed.free() => {
                            let (read, left) = rest.split_at(rest.len().min(piece_len()));
                            fed.give(read, &mut moved).unwrap();
                            rest = left;
                            false
                        }
                        _ => false,
                    };
                    if over {
                        put_replies.body = feeding.take().map(Feeding::let_go);
                    }
                }
                let now = hyper_rt::futures::now_ns();
                let body_read = feeding.is_none();
                put_over = put_over
                    || put_replies.write(&mut put_seen, body_read, now, &mut moved, &mut slowed)
                        == Wrote::Over;
                std::thread::yield_now();
            }
            assert_eq!(reply_seen(&put_seen).status, Status::Done);
            doors.give_replies(put_replies);
            if round == 0 {
                per_put = Some(gave);
            }
            let (get_half, mut get_replies) = doors.take().unwrap();
            get_replies.call = Some(lanes.caller());
            let get = Op::Get {
                bucket: "b".into(),
                key: "k".into(),
                headers: Headers::default(),
                version: None,
            };
            let read = scheming_gateway().charges.read(1);
            assert!(lanes.hand(handover(get_half, get, read, now)).is_none());
            let mut get_seen = Seen {
                credit: usize::MAX,
                ..Seen::default()
            };
            let mut get_over = false;
            while !(get_over && cell.ended == 2 * round + 2) {
                assert!(std::time::Instant::now() < until, "the get never ended");
                lanes
                    .serve_counting(&mut cell, |half| doors.give_native(half), &mut gave)
                    .unwrap();
                let now = hyper_rt::futures::now_ns();
                get_over = get_over
                    || get_replies.write(&mut get_seen, true, now, &mut moved, &mut slowed)
                        == Wrote::Over;
                std::thread::yield_now();
            }
            let get_reply = reply_seen(&get_seen);
            assert_eq!((get_reply.status, get_reply.size), (Status::Done, 1));
            doors.give_replies(get_replies);
        }
        let counters = rt.shutdown().unwrap();
        let shard = &counters[1];
        let per_put = per_put.unwrap();
        println!(
            "a put: {per_put:?}; {ROUNDS} puts and gets: {gave:?}, {} asks answered; the shard \
             polled {}, woken {} times from the test's thread, parked {}",
            cell.asked, shard.polls, shard.wakes_foreign, shard.waits
        );
        assert_eq!(gave.ends, 2 * ROUNDS);
        assert_eq!(
            cell.asked,
            u64::try_from(gave.asks).unwrap(),
            "each ask answered once"
        );
        assert!(
            gave.moved <= gave.asks + gave.ends,
            "the bytes moved ride what else the shard gave: {gave:?}"
        );
    }

    /// A host's wait is its own jobs' alone (node.md §2.9 step 6e), so a driving shard's task can
    /// wait on its host: none while it runs no job; a job whose ask is out is due at its client's
    /// patience, an S3 door's get holding no beat; and at once once whoever serves it holds the
    /// ask's answer, untaken.
    #[test]
    fn a_hosts_wait_is_its_own_jobs_alone() {
        let now = hyper_rt::futures::now_ns();
        let mut host = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
        let mut stub = Stub {
            answer: |_| panic!("nothing is asked"),
            answers: Vec::new(),
            asked: 0,
            no_room: false,
            table: None,
            ended: Vec::new(),
            abandoned: 0,
            takes_halves: false,
            half: None,
            arrived: Vec::new(),
        };
        assert_eq!(host.due(&stub), None, "no job, no wait");
        let get = work(
            Op::Get {
                bucket: "b".into(),
                key: "k".into(),
                headers: Headers::default(),
                version: None,
            },
            None,
        );
        let (mut job, _body, _answers) = local_job(get, now, 0);
        job.pending.push((Pending::Read(0), None));
        let patience = job.bound_ns();
        host.push(job, now);
        assert_eq!(host.due(&stub), Some(patience), "held to its patience");
        stub.answers.push((
            Pending::Read(0),
            Outcome::Read(ReadOutcome::Read(Box::new(Read::Current(None)))),
        ));
        let due = host.due(&stub).unwrap();
        assert!(
            (now..patience).contains(&due),
            "due at once once answered: {due}"
        );
    }

    /// The node lends its host out for each turn (`Node::lend`) and takes it back: the host taken
    /// leaves its default in its place, which asks the allocator for nothing, and the host given
    /// back keeps its jobs, its doors and its gateway's settings.
    #[test]
    fn a_hosts_lend_asks_the_allocator_for_nothing() {
        let gateway = std::sync::Arc::new(scheming_gateway());
        let mut held = Host::new(2, Some(std::sync::Arc::clone(&gateway)));
        let (native, replies) = held.doors.take().unwrap();
        held.doors.keep(native, replies);
        let (job, _, _) = local_job(Work::Draining(Box::new(Reply::status(Status::Done))), 0, 0);
        held.push(job, 0);
        hyper_measure::alloc::begin();
        let mut lent = std::mem::take(&mut held);
        assert!(held.idle() && held.gateway.is_none());
        lent.moved = 1;
        held = lent;
        let counts = hyper_measure::alloc::end();
        assert_eq!((counts.allocations, counts.reallocations), (0, 0));
        assert_eq!(
            (held.jobs.len(), held.doors.kept.len(), held.moved),
            (1, 1, 1)
        );
        assert!(
            held.gateway
                .is_some_and(|g| std::sync::Arc::ptr_eq(&g, &gateway))
        );
    }

    /// A native get still reading its version, its read of the Name range out and unanswered:
    /// its work held, its door native (handed, `handed_job`), its door's other half beside it.
    fn held_native_get(ask: u64, now: u64) -> (Job, Replies) {
        let get = work(
            Op::Get {
                bucket: "b".into(),
                key: "k".into(),
                headers: Headers::default(),
                version: None,
            },
            None,
        );
        let (mut job, replies) = handed_job(get, now, 0);
        job.pending.push((Pending::Read(ask), None));
        (job, replies)
    }

    /// A stub that answers nothing: every ask a job holds stays out.
    fn silent_stub() -> Stub {
        Stub {
            answer: |_| panic!("nothing is asked"),
            answers: Vec::new(),
            asked: 0,
            no_room: false,
            table: None,
            ended: Vec::new(),
            abandoned: 0,
            takes_halves: false,
            half: None,
            arrived: Vec::new(),
        }
    }

    /// The vouches each native reply's half passed to its exchange since the last call: the node's
    /// task's write after the host's step (`Node::write_replies`).
    fn vouches(replies: &mut [Replies], seen: &mut [Seen]) -> Vec<usize> {
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        replies
            .iter_mut()
            .zip(seen.iter_mut())
            .map(|(replies, seen)| {
                let before = seen.working;
                replies.write(seen, false, 0, &mut moved, &mut slowed);
                seen.working - before
            })
            .collect()
    }

    /// Native requests whose work is held and still, their asks out: each turn told every asker
    /// its work is held, as many calls into the transport as requests at every turn (the native
    /// GET bench's node, 2026-11-19); now each is told at its first turn held and then twice a beat.
    /// With the shard's clock still, as a test's thread reads it, turns after the first tell none.
    #[test]
    fn held_requests_are_vouched_for_at_their_first_turn_not_at_each() {
        const HELD: usize = 64;
        const TURNS: usize = 16;
        let mut host = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
        let mut stub = silent_stub();
        let mut replies = Vec::new();
        for ask in 0..HELD {
            let (job, half) = held_native_get(ask as u64, 0);
            host.push(job, 0);
            replies.push(half);
        }
        let mut seen: Vec<Seen> = (0..HELD).map(|_| Seen::default()).collect();
        let mut told = Vec::new();
        for _ in 0..TURNS {
            host.step(&mut stub).unwrap();
            told.push(vouches(&mut replies, &mut seen).iter().sum::<usize>());
        }
        let mut expected = vec![0; TURNS];
        expected[0] = HELD;
        assert_eq!(
            told, expected,
            "vouches a turn, {HELD} held: {HELD} a turn before"
        );
        assert_eq!(host.jobs.len(), HELD, "each still held");
    }

    /// A held request is vouched for half a beat after its last vouch (`vouch_every`), however
    /// often the node turns between, and at once at the turn after its work moved (research/77 D1,
    /// D2): at most two vouches a beat each while still, never more than half a beat between two
    /// where the node turns that often. Its host's next turn is due when its next vouch is
    /// (`Host::due`), so an idle node turns for it in time.
    #[test]
    fn a_held_request_is_vouched_for_within_each_beat_and_as_its_work_moves() {
        const HELD: usize = 8;
        const BEATS: u64 = 4;
        // Turns a beat apart (an idle node, turning when its host is due) and eight a beat (a
        // loaded one), the beat a probe timeout as the host's.
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap();
        for per_beat in [1, 8] {
            let mut host = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
            let mut stub = silent_stub();
            let mut replies = Vec::new();
            for ask in 0..HELD {
                let (job, half) = held_native_get(ask as u64, 1);
                host.push(job, 1);
                replies.push(half);
            }
            let mut seen: Vec<Seen> = (0..HELD).map(|_| Seen::default()).collect();
            let mut last: Vec<Option<u64>> = vec![None; HELD];
            let mut per_beat_told = vec![0usize; usize::try_from(BEATS).unwrap() + 1];
            let mut now = 1;
            while now <= 1 + BEATS * beat {
                host.bound_jobs(&mut stub, now, beat).unwrap();
                for (job, told) in vouches(&mut replies, &mut seen).into_iter().enumerate() {
                    assert!(told <= 1, "one vouch a turn at most");
                    if told == 1 {
                        if let Some(at) = last[job] {
                            let most = (beat / per_beat).max(vouch_every(beat));
                            assert!(now - at <= most, "job {job} vouched {} ns after", now - at);
                        }
                        last[job] = Some(now);
                        per_beat_told[usize::try_from((now - 1) / beat).unwrap()] += 1;
                    }
                }
                // Its next turn is due by its next vouch.
                let due = host.due(&stub).unwrap();
                let next = last.iter().flatten().min().unwrap() + vouch_every(beat);
                assert!(due <= next, "due {due}, next vouch {next}");
                now += beat / per_beat;
            }
            // Turning once a beat, each turn vouches; turning more often, two a beat each, the
            // last beat's bucket holding only its first turn.
            let mut expected =
                vec![HELD * if per_beat == 1 { 1 } else { 2 }; usize::try_from(BEATS).unwrap()];
            expected.push(HELD);
            assert_eq!(
                per_beat_told, expected,
                "{HELD} held, still, turning {per_beat} times a beat"
            );
        }

        // Work that moved is said at the next turn, between beats: once, by the one that moved.
        let mut host = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
        let mut stub = silent_stub();
        let mut replies = Vec::new();
        for ask in 0..HELD {
            let (job, half) = held_native_get(ask as u64, 1);
            host.push(job, 1);
            replies.push(half);
        }
        let mut seen: Vec<Seen> = (0..HELD).map(|_| Seen::default()).collect();
        host.bound_jobs(&mut stub, 1, beat).unwrap();
        assert_eq!(vouches(&mut replies, &mut seen), vec![1; HELD]);
        // Moved as a piece of its chunk moves, which keys it again (`Host::chunk_moved`).
        host.jobs[3].moved_ns = 2;
        host.rekey(3, 2);
        for now in [3, 4] {
            host.bound_jobs(&mut stub, now, beat).unwrap();
            let mut expected = vec![0; HELD];
            if now == 3 {
                expected[3] = 1;
            }
            assert_eq!(vouches(&mut replies, &mut seen), expected, "at {now}");
        }
    }

    /// A native put taking its body, nothing of its own out, tells its asker its work moves as its
    /// body is taken, half a beat apart at the most often (`vouch_every`), and nothing while its
    /// body stands still (research/77 D1, said as work moves). Holding nothing, it said nothing:
    /// CI's put of 20 MiB to a node on ubuntu-24.04-arm (2026-11-19) was named in its node's notes
    /// once in the 6,119 ms the node took 8,333,704 bytes of its body, 4.76 ms after it came, and
    /// its client's judgement rested on that note. Do: take its body each 9.3 ms, as that node
    /// did, for three beats, then stop. Expect: told first half a beat after its admission, then
    /// each half beat to within a piece, and once more at most after its body stops.
    #[test]
    fn a_native_put_taking_its_body_is_vouched_for_as_its_body_moves() {
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap();
        let every = vouch_every(beat);
        let piece = 9_300_000;
        let mut host = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
        let mut stub = silent_stub();
        let (job, half) = handed_job(Work::Put(native_put(20 << 20)), 1, 0);
        assert!(job.working() && !job.held());
        host.push(job, 1);
        let (mut replies, mut seen) = (vec![half], vec![Seen::default()]);
        let mut told = Vec::new();
        let mut now = 1;
        while now < 1 + 3 * beat {
            host.bound_jobs(&mut stub, now, beat).unwrap();
            if vouches(&mut replies, &mut seen) == [1] {
                told.push(now);
            }
            // Its body taken in its turn, which keys it again as it ends (`Host::turn_job`).
            host.jobs[0].moved_ns = now;
            host.rekey(0, now);
            now += piece;
        }
        assert!(told.len() >= 5, "told at {told:?}");
        let mut last = 1;
        for at in &told {
            assert!(
                at - last >= every && at - last < every + piece,
                "told {} ns after the last, at {told:?}",
                at - last
            );
            last = *at;
        }
        let mut after = 0;
        for _ in 0..8 {
            now += every;
            host.bound_jobs(&mut stub, now, beat).unwrap();
            after += vouches(&mut replies, &mut seen)[0];
        }
        assert!(after <= 1, "told {after} times with its body still");
    }

    /// Requests parked for want of room are each told they are held at the first turn they are
    /// held, then all at once half a beat apart (`Node::take_parked`, `vouch_every`), the node's
    /// turn due then (`Node::parked_due`): N parked and still tell 2N a beat, where every turn told
    /// N.
    #[test]
    fn parked_requests_are_vouched_for_twice_a_beat() {
        const PARKED: usize = 32;
        const BEATS: u64 = 3;
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap();
        // None told since none was parked: told at once.
        assert_eq!(parked_vouch_ns(None, 5, beat), 5);
        assert_eq!(parked_vouch_ns(Some(5), 6, beat), 5 + vouch_every(beat));
        let mut vouched = None;
        let mut told = vec![0usize; usize::try_from(BEATS).unwrap() + 1];
        let mut turns = 0;
        let mut now = 1;
        while now <= 1 + BEATS * beat {
            turns += 1;
            if now >= parked_vouch_ns(vouched, now, beat) {
                told[usize::try_from((now - 1) / beat).unwrap()] += PARKED;
                vouched = Some(now);
            }
            // The next turn comes by the next vouch.
            assert!(parked_vouch_ns(vouched, now, beat) <= now + vouch_every(beat));
            now += beat / 16;
        }
        assert!(turns > 16 * usize::try_from(BEATS).unwrap());
        // Two a beat, the last beat's bucket holding only its first turn.
        let mut expected = vec![2 * PARKED; usize::try_from(BEATS).unwrap()];
        expected.push(PARKED);
        assert_eq!(
            told,
            expected,
            "{PARKED} parked over {turns} turns: {} vouches a beat, {} before",
            2 * PARKED,
            PARKED * turns
        );
    }

    /// The peers' chunks a volume holds while their requests wait (`Storage::held`), idle: each
    /// turn told every one's asker, one call into the transport each per turn; now all at once at
    /// the first turn any is held, then half a beat apart (`vouch_held`), the node's next turn due
    /// by the next vouch (`Node::wait`). A chunk held beside others is told at their next vouch,
    /// within half a beat, and none is told once none is held.
    #[test]
    fn held_chunks_are_vouched_for_twice_a_beat_not_at_each_turn() {
        const HELD: usize = 64;
        const BEATS: u64 = 3;
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap();
        let every = vouch_every(beat);
        let mut held: Vec<usize> = (0..HELD).collect();
        let mut last: Vec<Option<u64>> = vec![None; HELD + 1];
        let mut vouched = None;
        let mut per_turn = Vec::new();
        let mut now = 1;
        while now <= 1 + BEATS * beat {
            // One more is held half way through, beside the others.
            if now >= 1 + beat + beat / 4 && held.len() == HELD {
                held.push(HELD);
                last[HELD] = Some(now);
            }
            let mut told = 0;
            vouch_held(&mut vouched, (now, beat), held.iter().copied(), |chunk| {
                if let Some(at) = last[chunk] {
                    assert!(
                        now - at <= every,
                        "chunk {chunk} told {} ns after",
                        now - at
                    );
                }
                last[chunk] = Some(now);
                told += 1;
            });
            per_turn.push(told);
            // The node's next turn is due at the next vouch, within half a beat (`Node::wait`).
            let next = vouched.unwrap() + every;
            assert!(
                next > now && next <= now + every,
                "next vouch {next} at {now}"
            );
            now += beat / 16;
        }
        let turns = per_turn.len();
        let told: usize = per_turn.iter().sum();
        let vouches = per_turn.iter().filter(|told| **told > 0).count();
        println!(
            "{HELD} held over {turns} turns: {told} vouches in {vouches} turns, {} before",
            HELD * turns
        );
        assert!(turns > 16 * usize::try_from(BEATS).unwrap());
        // Two vouches a beat, the first turn's and each half beat's after.
        assert_eq!(vouches, 2 * usize::try_from(BEATS).unwrap() + 1);
        assert_eq!(per_turn[0], HELD, "every one at the first turn held");
        assert!(per_turn.iter().all(|told| *told == 0 || *told >= HELD));

        // None held: none told, and the next held is told at its first turn.
        vouch_held(&mut vouched, (now + every, beat), None::<usize>, |_| {
            panic!("none held")
        });
        assert_eq!(vouched, None);
        let mut told = 0;
        vouch_held(&mut vouched, (now + every + 1, beat), Some(0), |_| {
            told += 1
        });
        assert_eq!((told, vouched), (1, Some(now + every + 1)));
    }

    thread_local! {
        /// The key comparisons [`Counted`] made on this test's thread.
        static COMPARED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    /// A job's key that counts each comparison made of it, as a search of every job's makes one
    /// for each job it passes.
    #[derive(Debug, Clone, Copy)]
    struct Counted(u64);

    impl std::hash::Hash for Counted {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.0.hash(state);
        }
    }

    impl PartialEq for Counted {
        fn eq(&self, other: &Self) -> bool {
            COMPARED.with(|compared| compared.set(compared.get() + 1));
            self.0 == other.0
        }
    }

    impl Eq for Counted {}

    /// A reply's job is found by its exchange in one probe of its host's slots (`Host::job_of`),
    /// kept as jobs are pushed and swap-removed (`Host::push`, `Host::remove`): with 4,096 jobs and
    /// every third ended, each lookup compares about one key, where the search of every job it
    /// replaces compared one a job passed, half the jobs a lookup.
    #[test]
    fn a_job_is_found_by_its_exchange_with_no_scan() {
        const JOBS: u64 = 4096;
        let mut slots = Slots::default();
        // Each slot's job, by its key, as the host's jobs hold them.
        let mut jobs: Vec<Counted> = Vec::new();
        for key in 0..JOBS {
            slots.hold(Some(Counted(key)), jobs.len());
            jobs.push(Counted(key));
        }
        let mut at = 0;
        while at < jobs.len() {
            if jobs[at].0.is_multiple_of(3) {
                let gone = jobs.swap_remove(at);
                slots.release(Some(gone));
                if let Some(moved) = jobs.get(at) {
                    slots.hold(Some(*moved), at);
                }
            } else {
                at += 1;
            }
        }
        COMPARED.with(|compared| compared.set(0));
        for (at, key) in jobs.iter().enumerate() {
            assert_eq!(slots.get(key), Some(at), "{key:?}");
        }
        let compared = COMPARED.with(std::cell::Cell::get);
        let scanned: usize = (1..=jobs.len()).sum();
        println!(
            "{} lookups: {compared} key comparisons, {scanned} by a search of every job",
            jobs.len()
        );
        assert!(
            compared <= 2 * jobs.len(),
            "{compared} comparisons for {} lookups",
            jobs.len()
        );
        for key in (0..JOBS).filter(|key| key.is_multiple_of(3)) {
            assert_eq!(slots.get(&Counted(key)), None, "ended");
        }
    }

    /// The asks the search `Host::chunk_moved` made before visited to find the job waiting on chunk
    /// ask `ask`: each job's asks, in order, until the one waiting on it (`Iterator::find` over
    /// every job, `any` over its asks).
    fn searched_for(host: &Host, ask: u64) -> usize {
        let mut visited = 0;
        for job in &host.jobs {
            for (pending, _) in &job.pending {
                visited += 1;
                if *pending == Pending::Chunk(ask) {
                    return visited;
                }
            }
        }
        visited
    }

    /// A piece of a chunk moving moves the job waiting on its ask, found in one probe of its host's
    /// asks' slots (`Host::chunk_moved`), kept as each ask is waited on, answered or let go and as
    /// jobs are pushed and swap-removed: 2,048 jobs each waiting on a read and a chunk, every third
    /// ended as `Host::remove` does, one answered and one let go, and each left told one probe,
    /// where the search of every job's asks it replaces visited each job's asks until the one
    /// waiting, about half of them a call.
    #[test]
    fn a_moved_chunks_job_is_found_by_its_ask_with_no_scan() {
        const JOBS: u64 = 2048;
        let mut host = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
        let mut halves = Vec::new();
        for ask in 0..JOBS {
            let (mut job, half) = held_native_get(ask, 1);
            job.pending.push((Pending::Chunk(ask), None));
            host.push(job, 1);
            halves.push(half);
        }
        let mut at = 0;
        while at < host.jobs.len() {
            let ask = match host.jobs[at].pending[0].0 {
                Pending::Read(ask) => ask,
                other => panic!("{other:?}"),
            };
            if ask.is_multiple_of(3) {
                host.remove(at).unwrap();
            } else {
                at += 1;
            }
        }
        // One job's read answered, no version, so its get is replied and its chunk let go; another
        // failed, its asks let go.
        let mut stub = silent_stub();
        stub.answers.push((
            Pending::Read(1),
            Outcome::Read(ReadOutcome::Read(Box::new(Read::Current(None)))),
        ));
        let waiting = |host: &Host, read| {
            host.jobs
                .iter()
                .position(|job| job.pending.first().map(|(p, _)| *p) == Some(Pending::Read(read)))
                .unwrap()
        };
        let answered = waiting(&host, 1);
        host.take_answers(&mut stub, answered, 1).unwrap();
        assert!(
            host.jobs[answered].pending.is_empty(),
            "replied, its chunk let go"
        );
        let replied = waiting(&host, 2);
        host.fail_job(&mut stub, replied, Status::Failed).unwrap();
        let left: Vec<(usize, u64)> = host
            .jobs
            .iter()
            .enumerate()
            .filter_map(|(at, job)| match job.pending.last()?.0 {
                Pending::Chunk(ask) => Some((at, ask)),
                _ => None,
            })
            .collect();
        assert_eq!(
            left.len(),
            host.jobs.len() - 2,
            "the replied jobs wait on nothing"
        );
        let searched: usize = left.iter().map(|(_, ask)| searched_for(&host, *ask)).sum();
        PROBES.with(|probes| probes.set(0));
        COMPARED_KEYS.with(|compared| compared.set(0));
        for (at, ask) in &left {
            host.jobs[*at].moved_ns = u64::MAX;
            host.chunk_moved(*ask);
            assert!(host.jobs[*at].moved_ns < u64::MAX, "job {at} moved");
        }
        let probes = PROBES.with(std::cell::Cell::get);
        let compared = COMPARED_KEYS.with(|compared| compared.replace(0));
        println!(
            "{} chunks moved over {} jobs: {probes} probes and {compared} keys compared keying \
             them, {searched} asks visited by a search of every job's",
            left.len(),
            host.jobs.len()
        );
        assert_eq!(probes, left.len(), "one probe a chunk moved");
        // Each keyed again, its entry moved at most the heap's depth, two keys compared a level.
        let depth = usize::try_from(host.jobs.len().ilog2()).unwrap() + 1;
        assert!(
            compared <= left.len() * 2 * (depth + 1),
            "{compared} compared"
        );
        // An ask no job waits on moves none, in one probe.
        PROBES.with(|probes| probes.set(0));
        host.chunk_moved(0);
        assert_eq!(PROBES.with(std::cell::Cell::get), 1);
        host.audit();
    }

    /// What the scan `Host::due` made before visits of `host`'s jobs, served by `stub`, at the
    /// shard's `now`, and the due it finds: every job's asks until one is answered, each against
    /// every answer waiting (the `Serve::answered` it called searched them); then every job once
    /// for its put's renewal and once more for its beat or bound.
    fn scanned_due(host: &Host, stub: &Stub, now: u64) -> (usize, Option<u64>) {
        let mut visited = 0;
        for job in &host.jobs {
            for (pending, _) in &job.pending {
                visited += 1;
                for (answer, _) in &stub.answers {
                    visited += 1;
                    if answer == pending {
                        return (visited, Some(now));
                    }
                }
            }
        }
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap();
        let wall = stub.wall_ns();
        let mut due = None;
        for job in &host.jobs {
            visited += 1;
            let renewal = job.renewal_ns().map(|at| now + at.saturating_sub(wall));
            due = [due, renewal].into_iter().flatten().min();
        }
        for job in &host.jobs {
            visited += 1;
            due = [due, job.beat_ns(now, beat)].into_iter().flatten().min();
        }
        (visited, due)
    }

    /// A turn's feeding visits the bodies something marked, not every put in flight: 1,024 native
    /// puts held, each given its first piece at its first look; then at each of 8 turns 16 jobs
    /// taking their piece, which frees the channel and the storage the next is read into, each
    /// named on the node's marks as it takes it (`Native::mark`, taken as `Node::take_marks`
    /// takes them), and given their next; and one job letting go of its body, visited to drop the
    /// rest. Before, each of the 9 turns walked every feeding held (`feed_bodies` over every
    /// slot): 9,216 visits. Now the first turn's 1,024, 16 at each turn after and the one let go
    /// of: 1,153. The oracle checks each change against a recount of what could move
    /// (`Feedings::audit`).
    #[test]
    fn a_turns_feeding_visits_the_bodies_that_moved_not_every_one() {
        const IN_FLIGHT: u64 = 1024;
        const READY: u64 = 16;
        const TURNS: u64 = 8;
        let piece = vec![3u8; piece_len()];
        let mut feedings: Feedings<u64> = Feedings::default();
        let mut natives = Vec::new();
        for exchange in 0..IN_FLIGHT {
            let (native, mut replies) = native_door().unwrap();
            let pieces = replies.body.take().unwrap();
            let handed = std::sync::Arc::clone(&native.handed);
            let length = u64::try_from(piece.len()).unwrap() * (TURNS + 2);
            let feeding = feed((length, None), pieces, handed, None);
            assert!(feedings.push((exchange, feeding)).is_none());
            natives.push((native, replies));
        }
        let mut moved = 0;
        // A visit as `Node::feed_body` makes one, of bytes its exchange always has: the piece
        // the job let go of first, then the next given while the storage is free.
        let mut turn = |feedings: &mut Feedings<u64>| {
            feedings.begin_visits();
            let mut visited = Vec::new();
            while let Some(at) = feedings.next_visit() {
                let (exchange, feeding) = feedings.at_mut(at).unwrap();
                feeding
                    .handed
                    .took
                    .store(0, std::sync::atomic::Ordering::Release);
                if feeding.job_let_go() {
                    feeding.held = None;
                    feeding.dropping = true;
                } else if feeding.free() {
                    assert_eq!(feeding.give(&piece, &mut moved), Ok(Fed::Moved));
                }
                visited.push(*exchange);
            }
            visited
        };
        let (mut before, mut after) = (0, 0);
        before += feedings.len();
        let first = turn(&mut feedings);
        assert_eq!(first.len(), usize::try_from(IN_FLIGHT).unwrap());
        after += first.len();
        let let_go = 9;
        for t in 0..TURNS {
            let taking: Vec<u64> = (0..READY)
                .map(|k| (t * READY + k * 61) % IN_FLIGHT)
                .collect();
            for &exchange in &taking {
                let (native, _) = &mut natives[usize::try_from(exchange).unwrap()];
                // The job takes its piece and lets it go, as `take_body` does, then names it.
                assert!(matches!(
                    native.body.pieces.try_recv(),
                    Ok(Some(Piece::Bytes(..)))
                ));
                native
                    .handed
                    .took
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                feedings.mark(exchange);
            }
            if t == 3 {
                natives[let_go].0.drop_body();
                feedings.mark(u64::try_from(let_go).unwrap());
            }
            before += feedings.len();
            let mut visited = turn(&mut feedings);
            visited.sort_unstable();
            let mut expected = taking.clone();
            if t == 3 {
                expected.push(u64::try_from(let_go).unwrap());
            }
            expected.sort_unstable();
            expected.dedup();
            assert_eq!(visited, expected, "turn {t} visits what moved alone");
            after += visited.len();
        }
        println!(
            "{IN_FLIGHT} bodies in flight, {READY} moving a turn: {after} visits over {} \
             feedings ({before} before)",
            TURNS + 1
        );
        assert_eq!(before, usize::try_from((TURNS + 1) * IN_FLIGHT).unwrap());
        assert_eq!(
            after,
            usize::try_from(IN_FLIGHT + TURNS * READY + 1).unwrap()
        );
        feedings.audit();
        assert_eq!(feedings.feeding.marked(), 0);
    }

    /// A turn's writing visits the replies something marked, not every reply in flight: 1,024
    /// native gets held, each job giving its head as it starts, then at each of 8 turns 16 jobs
    /// giving their next piece, each named on the node's marks as it gives (`Native::mark`, taken
    /// as `Node::take_marks` takes them), and one reply whose exchange took no more visited again
    /// only once its exchange says it takes more (`Event::Writable`). Before, each of the 10
    /// writings walked every reply held (`write_replies` over every slot): 10,240 visits. Now the
    /// first writing's 1,024, each reply's head, 16 at each turn after, and the one `Writable`
    /// named: 1,153. The oracle checks each change against a recount of what could move
    /// (`Writes::audit`).
    #[test]
    fn a_turns_writing_visits_the_replies_that_moved_not_every_one() {
        const IN_FLIGHT: u64 = 1024;
        const READY: u64 = 16;
        const TURNS: u64 = 8;
        let mut writes: Writes<u64> = Writes::default();
        let mut natives = Vec::new();
        let mut seen = Vec::new();
        for exchange in 0..IN_FLIGHT {
            let (native, replies) = native_door().unwrap();
            assert!(writes.push((exchange, replies)).is_none());
            natives.push(native);
            seen.push(Seen {
                credit: usize::MAX,
                ..Seen::default()
            });
        }
        let (mut moved, mut slowed) = (0, [0; door::Slowed::ALL.len()]);
        let mut turn = |writes: &mut Writes<u64>, seen: &mut [Seen]| {
            writes.begin_visits();
            let mut visited = Vec::new();
            while let Some((exchange, at)) = writes.next_visit() {
                let to = &mut seen[usize::try_from(exchange).unwrap()];
                let wrote = writes
                    .change_at(at, |replies| {
                        replies.write(to, true, 0, &mut moved, &mut slowed)
                    })
                    .unwrap();
                assert_ne!(wrote, Wrote::Over);
                visited.push(exchange);
            }
            visited
        };
        // Each job opens its get: its head given, its exchange named.
        for (exchange, native) in (0..IN_FLIGHT).zip(natives.iter_mut()) {
            open_get(native, b"fields", 64 * TURNS);
            writes.marked_by_job(exchange);
        }
        let (mut before, mut after) = (0, 0);
        before += writes.len();
        let first = turn(&mut writes, &mut seen);
        after += first.len();
        assert_eq!(first.len(), usize::try_from(IN_FLIGHT).unwrap());
        assert!(seen.iter().all(|seen| seen.answered.is_some()));
        // One exchange takes nothing more: its piece waits for its `Writable`.
        let stalled = 5;
        seen[stalled].credit = 0;
        for t in 0..TURNS {
            let giving: Vec<u64> = (0..READY)
                .map(|k| (t * READY + k * 61) % IN_FLIGHT)
                .collect();
            for &exchange in &giving {
                let native = &mut natives[usize::try_from(exchange).unwrap()];
                assert!(native.written(), "{exchange} wrote its last whole");
                assert_eq!(
                    native.give(Told::Bytes(Bytes::from(vec![7; 64]))),
                    Sent::Taken
                );
                writes.marked_by_job(exchange);
            }
            before += writes.len();
            let mut visited = turn(&mut writes, &mut seen);
            if t == 1 {
                // Its exchange takes more: said once, it is visited and its piece written.
                seen[stalled].credit = usize::MAX;
                writes.mark(u64::try_from(stalled).unwrap());
                before += writes.len();
                let writable = turn(&mut writes, &mut seen);
                assert_eq!(writable, vec![u64::try_from(stalled).unwrap()]);
                visited.extend(writable);
            }
            visited.sort_unstable();
            let mut expected = giving.clone();
            if t == 1 {
                expected.push(u64::try_from(stalled).unwrap());
            }
            expected.sort_unstable();
            expected.dedup();
            visited.dedup();
            assert_eq!(visited, expected, "turn {t} visits what moved alone");
            after += visited.len();
        }
        println!(
            "{IN_FLIGHT} replies in flight, {READY} moving a turn: {after} visits over {} \
             writings ({before} before)",
            TURNS + 2
        );
        assert_eq!(before, usize::try_from((TURNS + 2) * IN_FLIGHT).unwrap());
        assert_eq!(
            after,
            usize::try_from(IN_FLIGHT + TURNS * READY + 1).unwrap()
        );
        // No reply that could move is left unmarked, and nothing is marked once each was written.
        writes.audit();
        assert_eq!(writes.replies.marked(), 0);
    }

    /// The node's wait (`Node::host_due`) reads the earliest due of the native replies waiting for
    /// room in the endpoint's budget first of their heap (`Writes::first`): 2,048 replies in
    /// flight, every third waiting for room, each vouched for at its own time; then, a wait after
    /// each, each waiting one's vouch moved half a beat on and then its room come as its reply is
    /// written on, and every fifth of the others let go. Before, each wait walked every reply
    /// (`room_due_ns` of each). The due each finds is the same.
    #[test]
    fn a_nodes_wait_reads_no_reply_waiting_for_room() {
        const IN_FLIGHT: u64 = 2048;
        let beat = room_beat();
        let mut writes: Writes<u64> = Writes::default();
        for exchange in 0..IN_FLIGHT {
            let (_, replies) = native_door().unwrap();
            assert!(writes.push((exchange, replies)).is_none());
        }
        for exchange in (0..IN_FLIGHT).filter(|exchange| exchange % 3 == 0) {
            let vouched = (exchange * 7919) % (1 << 30);
            writes.change(exchange, |replies| replies.room = Some((1 << 40, vouched)));
        }
        COMPARED_KEYS.with(|compared| compared.set(0));
        let (mut waits, mut scanned, mut visited) = (0, 0, 0);
        let mut wait = |writes: &Writes<u64>| {
            let by_scan = writes
                .replies
                .iter()
                .filter_map(|(_, replies)| replies.room_due_ns(beat))
                .min();
            scanned += writes.len();
            assert_eq!(writes.first(), by_scan);
            visited += COMPARED_KEYS.with(|compared| compared.replace(0));
            waits += 1;
        };
        wait(&writes);
        let mut compared = 0;
        for exchange in 0..IN_FLIGHT {
            if exchange % 3 == 0 {
                writes.change(exchange, |replies| {
                    if let Some((_, vouched)) = replies.room.as_mut() {
                        *vouched += vouch_every(beat);
                    }
                });
                compared += COMPARED_KEYS.with(|compared| compared.replace(0));
                wait(&writes);
                writes.change(exchange, |replies| replies.room = None);
            } else if exchange % 5 == 1 {
                assert!(writes.remove(exchange).is_some());
            } else {
                continue;
            }
            compared += COMPARED_KEYS.with(|compared| compared.replace(0));
            wait(&writes);
        }
        println!(
            "{IN_FLIGHT} replies in flight, a third waiting for room: {waits} waits read {visited} \
             replies ({scanned} by the walk), the changes between them compared {compared} keys"
        );
        assert_eq!(visited, 0, "no reply read, no key compared");
        assert!(scanned > waits * 1700);
        assert_eq!(writes.first(), None, "no reply waits for room");
    }

    /// A host's wait reads none of its jobs (`Host::due`): with 2,048 held, each waiting on a read,
    /// its next turn is the earliest of its jobs' beats, read first of their heap, and an answer
    /// waiting is known for a job's by one probe of its asks' slots, an answer of no job's by one
    /// too; where the scan it replaces visited every job's asks, each against every answer waiting,
    /// and every job twice more. The due each finds is the same.
    #[test]
    fn a_hosts_wait_reads_none_of_its_jobs() {
        const JOBS: u64 = 2048;
        const OTHERS: u64 = 16;
        let mut host = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
        let mut stub = silent_stub();
        let mut halves = Vec::new();
        for ask in 0..JOBS {
            let (job, half) = held_native_get(ask, 0);
            host.push(job, 0);
            halves.push(half);
        }
        // Vouched for at its step, each keyed as its turn ended.
        host.step(&mut stub).unwrap();
        let now = hyper_rt::futures::now_ns();
        let visits = |host: &Host, stub: &Stub| {
            PROBES.with(|probes| probes.set(0));
            COMPARED_KEYS.with(|compared| compared.set(0));
            let due = host.due(stub);
            let visited =
                PROBES.with(std::cell::Cell::get) + COMPARED_KEYS.with(std::cell::Cell::get);
            (visited, due)
        };

        // Nothing answered: the earliest beat, its vouch half a beat after the step's.
        let (scanned, by_scan) = scanned_due(&host, &stub, now);
        let (visited, due) = visits(&host, &stub);
        let beat = u64::try_from(bounds::initial_pto().as_nanos()).unwrap();
        assert_eq!(due, Some(now + vouch_every(beat)));
        assert_eq!(due, by_scan);
        println!("{JOBS} held, none answered: {visited} visited, {scanned} by the scan");
        assert_eq!(visited, 0, "no job read, no key compared");
        assert_eq!(scanned, 3 * usize::try_from(JOBS).unwrap());

        // Answers waiting that are no job's (a repair's, a driving shard's), then the last job's.
        for other in 0..OTHERS {
            stub.answers.push((
                Pending::Command(other),
                Outcome::Command(CommandOutcome::Deadline),
            ));
        }
        let (scanned, by_scan) = scanned_due(&host, &stub, now);
        let (visited, due) = visits(&host, &stub);
        assert_eq!(due, by_scan);
        println!(
            "{JOBS} held, {OTHERS} answers of no job's: {visited} visited, {scanned} by the scan"
        );
        assert_eq!(
            visited,
            usize::try_from(OTHERS).unwrap(),
            "one probe an answer"
        );
        let last = host.jobs.last().unwrap().pending[0].0;
        stub.answers.push((
            last,
            Outcome::Read(ReadOutcome::Read(Box::new(Read::Current(None)))),
        ));
        let (scanned, by_scan) = scanned_due(&host, &stub, now);
        let (visited, due) = visits(&host, &stub);
        assert_eq!((due, by_scan), (Some(now), Some(now)), "due at once");
        println!(
            "{JOBS} held, the last's answer waiting: {visited} visited, {scanned} by the scan"
        );
        assert_eq!(visited, usize::try_from(OTHERS).unwrap() + 1);
        assert!(scanned > usize::try_from(JOBS * OTHERS).unwrap());

        // Its answer taken at the next step: the job replied, its keys let go, the rest kept.
        host.step(&mut stub).unwrap();
        assert_eq!(host.jobs.len(), usize::try_from(JOBS).unwrap() - 1);
        let (_, due) = visits(&host, &stub);
        assert_eq!(due, Some(now + vouch_every(beat)));
    }

    /// A host's keys (`Earliest`) against a plain list of each job's key, through a long run of
    /// jobs pushed, keyed, let go of their keys and swap-removed as `Host::push`, `Host::rekey` and
    /// `Host::remove` do, drawn from a fixed seed: after each, every slot's key and the least are
    /// the list's, each entry is named by its slot, and no key is less than its parent's.
    #[test]
    fn a_hosts_keys_agree_with_a_list_of_them() {
        // A fixed xorshift64 (Marsaglia, 2003): the same run each time.
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut draw = |below: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % below
        };
        let mut keys = Earliest::default();
        let mut listed: Vec<Option<u64>> = Vec::new();
        let mut most = 0;
        for _ in 0..20_000 {
            let drawn = draw(8);
            if drawn <= 2 && listed.len() < 512 {
                keys.grow();
                listed.push(None);
                let at = listed.len() - 1;
                let key = (draw(4) > 0).then(|| draw(64));
                keys.set(at, key);
                listed[at] = key;
            } else if drawn <= 5 && !listed.is_empty() {
                let at = usize::try_from(draw(listed.len() as u64)).unwrap();
                // Ties are common: keys from a small range.
                let key = (draw(5) > 0).then(|| draw(64));
                keys.set(at, key);
                listed[at] = key;
            } else if !listed.is_empty() {
                let at = usize::try_from(draw(listed.len() as u64)).unwrap();
                keys.swap_remove(at);
                listed.swap_remove(at);
            }
            most = most.max(listed.len());
            assert_eq!(keys.first(), listed.iter().flatten().min().copied());
            assert_eq!(keys.place.len(), listed.len());
            for (at, key) in listed.iter().enumerate() {
                assert_eq!(keys.key(at), *key, "slot {at}");
            }
            for (place, (key, at)) in keys.heap.iter().enumerate() {
                assert_eq!(keys.place[*at], Some(place));
                if place > 0 {
                    assert!(keys.heap[(place - 1) / 2].0 <= *key, "order at {place}");
                }
            }
            assert_eq!(keys.heap.len(), listed.iter().flatten().count());
        }
        assert_eq!(most, 512, "the run reached its most jobs");
    }

    /// A turn's handing reads the handovers alone (`Host::handing`, `Node::hand_off`): with 2,048
    /// jobs, every other one's fields read for a driving shard, each of those is taken out and
    /// read once, 1,024 reads, and the rest stay; a turn with none to hand reads none. The pass
    /// before looked at every job at each turn the node drives shards: 2,048 jobs at this turn
    /// (`looked <= JOBS + handed`, its test), and 2,048 again at each turn with none to hand.
    #[test]
    fn a_turns_handing_reads_the_handovers_alone() {
        const JOBS: usize = 2048;
        let mut host = Host::new(1, Some(std::sync::Arc::new(scheming_gateway())));
        let mut halves = Vec::new();
        for job in 0..JOBS {
            let op = Op::Get {
                bucket: "b".into(),
                key: format!("k{job}"),
                headers: Headers::default(),
                version: None,
            };
            let work = if job % 2 == 0 {
                Work::Handing(Box::new((op, None)))
            } else {
                work(op, None)
            };
            let (job, half) = handed_job(work, 0, 0);
            host.push(job, 0);
            halves.push(half);
        }
        let _ = reads();
        let mut handed = 0;
        while let Some((handover, _)) = host.handing() {
            assert!(matches!(handover.op, Op::Get { .. }));
            handed += 1;
        }
        assert_eq!(handed, JOBS / 2);
        assert_eq!(
            reads(),
            (JOBS / 2) as u64,
            "each handover read once, no other job"
        );
        assert!(host.handing().is_none());
        assert_eq!(reads(), 0, "a turn with none to hand reads none");
        assert_eq!(host.jobs.len(), JOBS / 2);
        assert!(
            host.jobs
                .iter()
                .all(|job| !matches!(job.work, Work::Handing(_)))
        );
        host.audit_keys(0);
    }
}
