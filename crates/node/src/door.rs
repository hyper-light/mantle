//! The S3 listener's door into the node's object host (docs/design/node.md §4.2): an operation
//! handed over in-process, with no loopback QUIC hop. The listener submits a [`Request`], the
//! typed operation its HTTP request asks with the principal its signature names, on the node's
//! [`Doorway`]; the request's body follows as [`Piece`]s on a bounded channel the connection fills
//! only as the host takes them, so TCP carries the backpressure, and its end comes only once the
//! connection has checked what the body's signature and checksum say. The reply goes back as an
//! [`Answer`]: its head, then a get's bytes, on a bounded channel the connection drains to its
//! socket, so the host writes no faster than the client reads. Each send wakes the other side's
//! task: the node's, whose turn takes what came, through the waker the doorway holds.
//!
//! The body's channel holds as many pieces as the node learned a body needs in flight ([`Depth`]):
//! by Little's law, the host's time to take a piece over the connection's time to deliver one,
//! each measured on the pieces themselves, so the body comes at its client's pace and the node
//! holds no piece more. A reply's channel holds as many answers as the node learned a reply needs
//! in flight ([`Replying`]): the host's time from one turn's answers to the next's over the
//! connection's time to write one, from the turn each piece carries and the connection's own clock.
//!
//! Before its operation, a request signed with a principal's key asks the node for that key
//! ([`KeyAsk`]): the node reads the key's row and its principal's from the root range by
//! ReadIndex and unwraps the key's secret under the root key, so the connection checks the
//! signature against what the cell holds as the request arrives (docs/design/principals.md).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::Waker;
use std::time::Instant;

use bytes::Bytes;
use hyper_rt::RtError;
use hyper_rt::sync::{ChannelReceiver, Sender, SyncError, channel};
use mantle_s3::checksum::Checksum;

/// Why a request was answered `503 SlowDown`, the protocol's causes, which the S3 door's
/// `<Message>` names after S3's words (`listener::respond::slowed`).
pub use mantle_protocol::object::Slowed;
use mantle_protocol::object::{Op, Reply, Status};

/// What the listener hands the node: an operation, boxed as the larger of the two, or a key to look
/// up.
pub enum Asked {
    Op(Box<Request>),
    Key(KeyAsk),
}

/// A principal's access key a request is signed with, asked of the node before the request's
/// signature is checked; the answer comes on `reply`, which holds one.
pub struct KeyAsk {
    pub id: String,
    pub reply: Sender<Signed>,
}

/// What the node answers a key's lookup.
pub enum Signed {
    /// The key is a principal's: what its requests are judged by.
    Principal(Box<Signer>),
    /// The cell holds no such key, or held it once and it was revoked: S3's
    /// `InvalidAccessKeyId`.
    Unknown,
    /// The lookup was not answered: the root range's rows failing (`Failed`).
    Refused(Status),
    /// The lookup was not answered, for a cause that passes (`503 SlowDown`): which.
    Slowed(Slowed),
}

/// A principal's key as the root range holds it, its secret unwrapped: the secret, wiped when
/// dropped, and the principal's name, unique ID and identity policy's compact form.
pub struct Signer {
    pub secret: mantle_s3::account::Secret,
    pub name: String,
    pub id: String,
    pub policy: String,
}

/// An operation the listener hands the host.
pub struct Request {
    pub op: Op,
    /// Who asks: the account its access key acts for (`mantle init`).
    pub principal: String,
    /// The places of a DeleteObjects' objects its principal's identity policy does not let it
    /// delete, each answered `AccessDenied` in its place, its delete never asked (research/91 §6);
    /// none for any other operation, and none of the root's. At most the objects it names.
    pub denied: Vec<usize>,
    /// The request's body, as the connection reads it.
    pub body: ChannelReceiver<Piece>,
    /// The reply, as the host writes it.
    pub reply: Sender<Answer>,
}

/// A piece of a request's body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Piece {
    /// Bytes of the body, and how the connection delivered them, which the host learns the body's
    /// depth from ([`Doorway::took`]).
    Bytes(Bytes, Paced),
    /// The body ended whole, its signature's and its checksum's word held: the checksum an
    /// aws-chunked body's trailer carried, where it carried one (05 §3.2).
    End(Option<Checksum>),
    /// The body did not hold as it streamed: its signature, its digest or its framing, the status
    /// that says which; nothing of it commits.
    Refused(Status),
}

/// How the connection delivered a piece of body: when it sent it, and the time it took to deliver
/// it, from its last piece's send to this one's bytes ready, none of it waiting for the queue's
/// room; none for a body's first piece, whose time is its client's answer to `100 Continue` as
/// much as its pace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Paced {
    pub sent: Instant,
    pub delivered_ns: Option<u64>,
}

/// The pieces a body's channel holds, learned from the pieces themselves (docs/design/node.md
/// §4.3). By Little's law (Little 1961) the pieces in the channel at once for a body to come at its
/// client's pace are the rate it comes at times the time each waits there: the host's time to take
/// a piece, from its send to the host's turn taking it, over the connection's time to deliver one,
/// rounded up and at least one. Each time is summed over a phase of as many pieces as admission's
/// phase measures (`admit::PHASE`): the ratio of two such sums tells a quarter's change from chance
/// as admission's ratio of two phases' powers does, its log error `√(2/n)`. A phase's depth
/// replaces the last's, so what a phase measured is forgotten the phase after, and the sample is
/// at most a phase's pieces. Until a phase is measured the depth is one, the least that moves a
/// body: the host takes one piece while the connection reads the next into its own buffer, and a
/// put holds no more than a request within one segment (`listener::REQUEST_BYTES`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Depth {
    pieces: u64,
    taken_ns: u128,
    delivered_ns: u128,
}

impl Depth {
    /// The depth before any phase is measured: one piece.
    pub const FIRST: usize = 1;

    /// A piece the host took `taken_ns` after its send, which the connection took `delivered_ns`
    /// to deliver, a delivery too quick for the clock to time a nanosecond's: the phase's depth
    /// once this piece ends it.
    pub fn took(&mut self, taken_ns: u64, delivered_ns: u64) -> Option<usize> {
        self.pieces = self.pieces.saturating_add(1);
        self.taken_ns = self.taken_ns.saturating_add(u128::from(taken_ns));
        self.delivered_ns = self
            .delivered_ns
            .saturating_add(u128::from(delivered_ns.max(1)));
        if self.pieces < crate::admit::PHASE {
            return None;
        }
        let depth = self.taken_ns.div_ceil(self.delivered_ns.max(1)).max(1);
        *self = Self::default();
        Some(usize::try_from(depth).unwrap_or(usize::MAX))
    }
}

/// The answers a reply's channel holds, learned from the answers themselves (docs/design/node.md
/// §4.3): the body's [`Depth`] turned round, the host giving and the connection taking. By Little's
/// law (Little 1961) the answers in the channel at once for a reply to go at its client's pace are
/// the rate the connection writes them times the time each waits to be given: the host's mean time
/// from one turn's answers to the next turn's, over the connection's mean time to write one, from
/// taking it to its last octet written, none of it waiting for the host; rounded up, and at least
/// one. The host's time is a turn's, not a piece's: a turn sends every piece the channel has room
/// for at once (`Node::write_get`), so within a turn the time between two is none, and its pieces
/// wait for the turn together. The ratio of the means is the ratio of the sums times the pieces a
/// turn gives, as measured: `⌈(Σ given × pieces) / (Σ written × turns)⌉`. The connection writes a
/// turn's pieces before the next turn comes, or waits for it, so the time between turns is at least
/// their writing and the depth holds a turn's group. A phase is as many turns as admission's phase
/// measures (`admit::PHASE`), turns the fewer samples of the two, and its depth replaces the last's.
/// Until a phase is measured the depth is one, the least that moves a reply: its head, or the piece
/// the host gives while the connection writes the last.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Replying {
    turns: u64,
    pieces: u64,
    given_ns: u128,
    written_ns: u128,
}

impl Replying {
    /// The depth before any phase is measured: one answer.
    pub const FIRST_ANSWERS: usize = 1;

    /// Turns of the host's that gave their answers `given_ns` after the turns before them, in all,
    /// and pieces the connections wrote in `written_ns`, in all: the phase's depth once these end
    /// it. A product past a `u128` saturates, a depth past any cap.
    pub fn add(
        &mut self,
        turns: u64,
        pieces: u64,
        given_ns: u64,
        written_ns: u64,
    ) -> Option<usize> {
        self.turns = self.turns.saturating_add(turns);
        self.pieces = self.pieces.saturating_add(pieces);
        self.given_ns = self.given_ns.saturating_add(u128::from(given_ns));
        self.written_ns = self.written_ns.saturating_add(u128::from(written_ns));
        if self.turns < crate::admit::PHASE {
            return None;
        }
        let given = self.given_ns.saturating_mul(u128::from(self.pieces.max(1)));
        let written = self
            .written_ns
            .max(1)
            .saturating_mul(u128::from(self.turns));
        let depth = given.div_ceil(written).max(1);
        *self = Self::default();
        Some(usize::try_from(depth).unwrap_or(usize::MAX))
    }
}

/// What the node learned of its doors' queues, which the listener's connections read as they make
/// a request's channels: the depth of a body's ([`Depth`]) and of a reply's ([`Replying`]), the
/// host their one writer; what the connections wrote of their replies since the host last took it
/// (`Doorway::replies`); and what their gets' clients took once their sockets were full
/// (`Doorway::drained`); each added to by the connections and taken by the host alone, an atomic
/// that needs no lock. A sample adds its time and then its count, so the host may take its time a
/// turn before its count: a phase may hold a turn's time more or less for each connection writing
/// as it ends, among its 267 turns, and a pace the time of a piece more or less for each connection
/// writing as the host takes it. A sum holds what the connections wrote between two of the host's
/// turns, so it wraps only past 2^64 nanoseconds or octets of it, centuries.
#[derive(Default)]
struct Learned {
    body: AtomicUsize,
    reply: AtomicUsize,
    turns: AtomicU64,
    pieces: AtomicU64,
    given_ns: AtomicU64,
    written_ns: AtomicU64,
    drained: AtomicU64,
    drained_ns: AtomicU64,
}

/// A piece of a reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    /// A put's driver wants its body: every check that could refuse it before a byte of it is read
    /// has passed, so the listener sends `100 Continue` to a client that waits for it (RFC 9110
    /// §10.1.1; node.md §4.5).
    Ready,
    /// The reply: its status and the operation's facts.
    Head(Box<Reply>),
    /// A get's bytes, in order, and the host's turn that sent them, one instant for every piece a
    /// turn sends, which the node learns the reply's depth from ([`Replying`]).
    Bytes(Bytes, Instant),
    /// Every byte written.
    End,
    /// The operation failed past its head: the connection ends with the response unfinished, as
    /// the native door refuses its exchange, so no client takes a short body for the object's.
    Failed,
    /// The reply in place of a head: `503 SlowDown`, for its cause.
    Slowed(Slowed),
}

/// The node's end of its door: the requests the listener submits, and the waker that turns the
/// node to take them.
pub struct Doorway {
    requests: ChannelReceiver<Asked>,
    /// One request taken from the channel to learn that one waits, ahead of the rest.
    held: Option<Asked>,
    slowed: Arc<Slowings>,
    /// The phases of a body's pieces and of a reply's turns under way, and what the last of each
    /// learned, which the listener's connections read as they make a request's channels.
    depth: Depth,
    replying: Replying,
    learned: Arc<Learned>,
}

/// The listener's end: where it submits requests, and the waker of the node that takes them.
#[derive(Clone)]
pub struct Submitter {
    requests: Sender<Asked>,
    node: Waker,
    slowed: Arc<Slowings>,
    learned: Arc<Learned>,
}

/// The S3 requests the listener answered `503 SlowDown`, a count for each cause in
/// [`Slowed::ALL`]'s order, which the node says in its lines: shared by the listener's connections,
/// which count, and the node's task, which reads, each count an atomic `u64` that needs no lock.
/// One is added per answer, so a count wraps only past 2^64 answers, centuries at any rate a
/// listener answers.
#[derive(Default)]
struct Slowings([AtomicU64; Slowed::ALL.len()]);

/// A door for `capacity` requests waiting at once, past which a submission is refused at once
/// (`503 SlowDown`, node.md §4.5), for the node whose task `node` wakes.
pub fn doorway(capacity: usize, node: Waker) -> Result<(Doorway, Submitter), RtError> {
    let (requests, taken) = channel(capacity)?;
    let slowed = Arc::new(Slowings::default());
    let learned = Arc::new(Learned {
        body: AtomicUsize::new(Depth::FIRST),
        reply: AtomicUsize::new(Replying::FIRST_ANSWERS),
        ..Learned::default()
    });
    Ok((
        Doorway {
            requests: taken,
            held: None,
            slowed: Arc::clone(&slowed),
            depth: Depth::default(),
            replying: Replying::default(),
            learned: Arc::clone(&learned),
        },
        Submitter {
            requests,
            node,
            slowed,
            learned,
        },
    ))
}

impl Doorway {
    /// The next request submitted, if one waits; none too once the listener is gone.
    pub fn take(&mut self) -> Option<Asked> {
        self.held
            .take()
            .or_else(|| self.requests.try_recv().ok().flatten())
    }

    /// The request that waits to be taken next, held ahead of the rest until it is; none where
    /// none waits.
    pub fn waiting(&mut self) -> Option<&Asked> {
        if self.held.is_none() {
            self.held = self.requests.try_recv().ok().flatten();
        }
        self.held.as_ref()
    }

    /// The host took a piece of body the connection delivered as `paced` says: a sample of the
    /// depth's phase ([`Depth`]), and the phase's depth for the connections once it ends. A body's
    /// first piece, whose delivery is not timed, is no sample.
    pub fn took(&mut self, paced: Paced) {
        let Some(delivered_ns) = paced.delivered_ns else {
            return;
        };
        let taken_ns = u64::try_from(paced.sent.elapsed().as_nanos()).unwrap_or(u64::MAX);
        if let Some(depth) = self.depth.took(taken_ns, delivered_ns) {
            self.learned.body.store(depth, Ordering::Relaxed);
        }
    }

    /// Takes what the connections wrote of their replies since the host last did: samples of the
    /// reply's phase ([`Replying`]), and the phase's depth for the connections once it ends. The
    /// host takes them each turn, before the door's requests.
    pub fn replies(&mut self) {
        let learned = &self.learned;
        // The counts before the times, the order a connection adds them in turned round: a count
        // taken is never without its time, which its connection added before it (`Release`, seen
        // by the `Acquire` here); a time added between the two waits for its count a turn.
        let turns = learned.turns.swap(0, Ordering::Acquire);
        let pieces = learned.pieces.swap(0, Ordering::Acquire);
        let given_ns = learned.given_ns.swap(0, Ordering::Relaxed);
        let written_ns = learned.written_ns.swap(0, Ordering::Relaxed);
        if let Some(depth) = self.replying.add(turns, pieces, given_ns, written_ns) {
            learned.reply.store(depth, Ordering::Relaxed);
        }
    }

    /// Takes what the connections' gets' clients took of their replies' pieces once each socket
    /// was full since the host last did: the octets and the time they took, summed over the pieces,
    /// the clients' pace for the window a get holds being read (`crate::window`); none where no
    /// piece filled its socket. The host takes it each turn, beside the replies' samples.
    pub fn drained(&mut self) -> Option<(u64, u64)> {
        let learned = &self.learned;
        // The octets before the time, the order a connection adds them in turned round, as
        // `replies` takes its counts.
        let octets = learned.drained.swap(0, Ordering::Acquire);
        let ns = learned.drained_ns.swap(0, Ordering::Relaxed);
        if octets > 0 && ns > 0 {
            Some((octets, ns))
        } else {
            None
        }
    }

    /// The S3 requests the listener answered `503 SlowDown`, for each cause in [`Slowed::ALL`]'s
    /// order.
    pub fn slowed(&self) -> [u64; Slowed::ALL.len()] {
        let mut counts = [0; Slowed::ALL.len()];
        for (count, kept) in counts.iter_mut().zip(&self.slowed.0) {
            *count = kept.load(Ordering::Relaxed);
        }
        counts
    }
}

/// Why the door took no request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// It holds as many as it takes.
    Full,
    /// The node is gone, or the submitter is off the node's shard.
    Gone,
}

impl Submitter {
    /// Submits `request` and turns the node to take it; refused when the door holds as many as
    /// it takes or the node is gone, the request dropped.
    pub fn submit(&self, request: Request) -> Result<(), Refusal> {
        self.send(Asked::Op(Box::new(request)))
    }

    /// Asks the node for a key, as [`Submitter::submit`] hands it an operation.
    pub fn look_up(&self, ask: KeyAsk) -> Result<(), Refusal> {
        self.send(Asked::Key(ask))
    }

    fn send(&self, asked: Asked) -> Result<(), Refusal> {
        match self.requests.try_send(asked) {
            Ok(()) => {
                self.node.wake_by_ref();
                Ok(())
            }
            Err(SyncError::Full(_)) => Err(Refusal::Full),
            Err(SyncError::Closed(_) | SyncError::NotOnShardThread(_)) => Err(Refusal::Gone),
        }
    }

    /// Turns the node, after the connection sent a piece of a body it took.
    pub fn wake(&self) {
        self.node.wake_by_ref();
    }

    /// The pieces the node last learned a body needs in flight to its host ([`Depth`]), which a
    /// connection caps at what its memory holds as it makes a body's channel.
    pub fn depth(&self) -> usize {
        self.learned.body.load(Ordering::Relaxed)
    }

    /// The answers the node last learned a reply needs in flight from its host ([`Replying`]), which
    /// a connection caps at what its memory holds as it makes a reply's channel.
    pub fn reply_depth(&self) -> usize {
        self.learned.reply.load(Ordering::Relaxed)
    }

    /// A turn's pieces of a reply the connection wrote: `pieces` of them in `written_ns`, each
    /// timed from its taking to its last octet written and at least a nanosecond, and the host's
    /// time from its turn before to the turn that gave them, none for a reply's first turn, whose
    /// time is its driver's first read. The node's host takes them at its next turn
    /// ([`Doorway::replies`]).
    pub fn replied(&self, given_ns: Option<u64>, pieces: u64, written_ns: u64) {
        let learned = &self.learned;
        // Each time before its count (`Doorway::replies`).
        learned.written_ns.fetch_add(written_ns, Ordering::Relaxed);
        learned.pieces.fetch_add(pieces, Ordering::Release);
        if let Some(given_ns) = given_ns {
            learned.given_ns.fetch_add(given_ns, Ordering::Relaxed);
            learned.turns.fetch_add(1, Ordering::Release);
        }
    }

    /// A get's client took `octets` of a piece of its reply in `ns` once the connection's socket
    /// was full (`listener::io::Drained`): its pace, which the node's host takes at its next turn
    /// ([`Doorway::drained`]).
    pub fn drained(&self, octets: u64, ns: u64) {
        let learned = &self.learned;
        // The time before its octets (`Doorway::drained`).
        learned.drained_ns.fetch_add(ns, Ordering::Relaxed);
        learned.drained.fetch_add(octets, Ordering::Release);
    }

    /// Counts an S3 request the listener answers `503 SlowDown` for `cause`, which the node says
    /// in its lines; `cause` back, for the answer that names it.
    pub fn slowed(&self, cause: Slowed) -> Slowed {
        if let Some(count) = self.slowed.0.get(cause.index()) {
            count.fetch_add(1, Ordering::Relaxed);
        }
        cause
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A phase's pieces, each taken `taken_ns` after its send and delivered in `delivered_ns`: the
    /// depth the phase's last piece gives, none before it.
    fn phase(depth: &mut Depth, taken_ns: u64, delivered_ns: u64) -> Option<usize> {
        for _ in 1..crate::admit::PHASE {
            assert_eq!(depth.took(taken_ns, delivered_ns), None);
        }
        depth.took(taken_ns, delivered_ns)
    }

    /// The host's time to take a piece over the connection's time to deliver one, rounded up,
    /// once a phase is measured; never below one; each phase's depth its own.
    #[test]
    fn the_depth_is_the_pieces_delivered_while_the_host_takes_one() {
        let mut depth = Depth::default();
        // 300 µs to take, 100 µs to deliver: three pieces in flight.
        assert_eq!(phase(&mut depth, 300_000, 100_000), Some(3));
        // 250 µs over 100 µs, rounded up.
        assert_eq!(phase(&mut depth, 250_000, 100_000), Some(3));
        // A host faster than its connection, or one that takes at once: one.
        assert_eq!(phase(&mut depth, 10_000, 100_000), Some(1));
        assert_eq!(phase(&mut depth, 0, 100_000), Some(1));
        // A delivery too quick to time is a nanosecond's.
        assert_eq!(phase(&mut depth, 7, 0), Some(7));
        // A phase forgets the last: the slow phase before says nothing of this one.
        assert_eq!(phase(&mut depth, 1_000_000, 1_000), Some(1_000));
        assert_eq!(phase(&mut depth, 1_000, 1_000), Some(1));
    }

    /// A phase's turns, each `given_ns` after the turn before, with `pieces` pieces a turn written
    /// in `written_ns` each: the depth the phase's last turn gives, none before it.
    fn turns(
        replying: &mut Replying,
        given_ns: u64,
        pieces: u64,
        written_ns: u64,
    ) -> Option<usize> {
        let written = pieces * written_ns.max(1);
        for _ in 1..crate::admit::PHASE {
            assert_eq!(replying.add(1, pieces, given_ns, written), None);
        }
        replying.add(1, pieces, given_ns, written)
    }

    /// A reply's depth is the host's time from one turn's answers to the next's over the
    /// connection's time to write one, rounded up, once a phase of turns is measured: never below
    /// one, a turn's group held where the connection writes it before the next turn, and each phase
    /// its own.
    #[test]
    fn a_replys_depth_is_the_answers_written_while_the_host_gives_a_turns() {
        let mut replying = Replying::default();
        // Turns 300 µs apart, one piece a turn written in 100 µs: three in flight.
        assert_eq!(turns(&mut replying, 300_000, 1, 100_000), Some(3));
        // 250 µs over 100 µs, rounded up.
        assert_eq!(turns(&mut replying, 250_000, 1, 100_000), Some(3));
        // A host whose turns come quicker than a piece is written, or at once: one.
        assert_eq!(turns(&mut replying, 10_000, 1, 100_000), Some(1));
        assert_eq!(turns(&mut replying, 0, 1, 100_000), Some(1));
        // Turns of three pieces each, written in 100 µs, 300 µs apart: the turn's group, three.
        assert_eq!(turns(&mut replying, 300_000, 3, 100_000), Some(3));
        // The same turns to a connection that writes in half the time: twice the group.
        assert_eq!(turns(&mut replying, 300_000, 3, 50_000), Some(6));
        // A write too quick to time is a nanosecond's.
        assert_eq!(turns(&mut replying, 7, 1, 0), Some(7));
        // A phase forgets the last: the slow phase before says nothing of this one.
        assert_eq!(turns(&mut replying, 1_000_000, 1, 1_000), Some(1_000));
        assert_eq!(turns(&mut replying, 1_000, 1, 1_000), Some(1));
        // Times past any sum saturate: a depth past any cap, which the connection caps.
        assert_eq!(turns(&mut replying, u64::MAX, 1, 1), Some(usize::MAX));
        // A phase is its turns, not its pieces: none before them, however many pieces.
        let mut replying = Replying::default();
        assert_eq!(replying.add(0, 10 * crate::admit::PHASE, 0, 0), None);
        assert_eq!(replying.add(crate::admit::PHASE - 1, 0, 0, 0), None);
        // 10 ns a turn over 1 ns a piece.
        let pieces = 10 * crate::admit::PHASE;
        assert_eq!(
            replying.add(1, 0, 10 * crate::admit::PHASE, pieces),
            Some(10)
        );
    }

    /// A connection reads the reply's depth the node learned from what its connections wrote: one
    /// before a phase, a reply's first turn no sample, nothing until the host takes it, and a
    /// phase's depth once it has.
    #[test]
    fn a_connection_reads_the_reply_depth_the_node_learned() {
        let (mut doorway, submitter) = doorway(1, Waker::noop().clone()).unwrap();
        assert_eq!(submitter.reply_depth(), Replying::FIRST_ANSWERS);
        for _ in 0..crate::admit::PHASE {
            submitter.replied(None, 1, 1_000);
        }
        doorway.replies();
        assert_eq!(submitter.reply_depth(), Replying::FIRST_ANSWERS);
        // Turns 4 ms apart, two pieces each written in 1 ms: four in flight.
        for _ in 0..crate::admit::PHASE {
            submitter.replied(Some(4_000_000), 2, 2_000_000);
        }
        assert_eq!(submitter.reply_depth(), Replying::FIRST_ANSWERS);
        doorway.replies();
        // The first phase's pieces with no turn wrote quicker, so the depth is past four.
        assert!(submitter.reply_depth() >= 4, "{}", submitter.reply_depth());
        for _ in 0..crate::admit::PHASE {
            submitter.replied(Some(4_000_000), 2, 2_000_000);
        }
        doorway.replies();
        assert_eq!(submitter.reply_depth(), 4);
    }

    /// The listener's connections read the depth the node learned: one before any phase, a body's
    /// first piece no sample, and a phase's depth once it ends.
    #[test]
    fn a_connection_reads_the_depth_the_node_learned() {
        let (mut doorway, submitter) = doorway(1, Waker::noop().clone()).unwrap();
        assert_eq!(submitter.depth(), Depth::FIRST);
        let long_ago = Instant::now()
            .checked_sub(std::time::Duration::from_millis(10))
            .unwrap();
        for _ in 0..crate::admit::PHASE {
            doorway.took(Paced {
                sent: long_ago,
                delivered_ns: None,
            });
        }
        assert_eq!(submitter.depth(), Depth::FIRST);
        for _ in 0..crate::admit::PHASE {
            doorway.took(Paced {
                sent: long_ago,
                delivered_ns: Some(1_000_000),
            });
        }
        // Taken 10 ms or more after its send, each delivered in 1 ms.
        assert!(submitter.depth() >= 10, "{}", submitter.depth());
    }
}
