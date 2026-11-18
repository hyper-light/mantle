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
//! Before its operation, a request signed with a principal's key asks the node for that key
//! ([`KeyAsk`]): the node reads the key's row and its principal's from the root range by
//! ReadIndex and unwraps the key's secret under the root key, so the connection checks the
//! signature against what the cell holds as the request arrives (docs/design/principals.md).

use std::task::Waker;

use bytes::Bytes;
use hyper_rt::RtError;
use hyper_rt::sync::{ChannelReceiver, Sender, SyncError, channel};
use mantle_s3::checksum::Checksum;

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

/// Why an S3 request was answered `503 SlowDown`, each a cause that passes, which stock clients ask
/// again (node.md §4.5). Its `<Code>` is `SlowDown`, as the S3 API reference states for 503; its
/// `<Message>` says which path answered, so a refusal names its cause ([`Slowed::message`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slowed {
    /// Past the connections the listener serves at once (`listener::Settings::connections`).
    Connections,
    /// The node's door held as many requests as it keeps, the node was gone, or its channels for
    /// the request could not be made.
    Door,
    /// The node held as many key lookups as its door holds requests.
    Lookups,
    /// The node was stopping, or had no room for an operation its door handed it.
    Stopping,
    /// The access key's read of the root range waited its range's budget without a leader that
    /// could serve it (node.md §5.4).
    KeyLeaderless,
    /// A read or command of the operation waited its range's budget without a leader that could
    /// serve it (node.md §5.4).
    Leaderless,
    /// Fewer of a block's chunks could be read than rebuild it.
    Unreadable,
    /// No volume the placement offered took one of a block's chunks.
    Unplaced,
    /// The operation's work was held past a stock client's patience (research/77 D2).
    Patience,
}

impl Slowed {
    /// What the refusal's `<Message>` says past S3's own words: which path answered.
    pub fn message(self) -> &'static str {
        match self {
            Self::Connections => {
                "Please reduce your request rate. Past the connections the node serves at once."
            }
            Self::Door => {
                "Please reduce your request rate. The node's door was full, or the node gone."
            }
            Self::Lookups => {
                "Please reduce your request rate. The node held as many key lookups as it keeps."
            }
            Self::Stopping => "Please reduce your request rate. The node was stopping.",
            Self::KeyLeaderless => {
                "Please reduce your request rate. The access key's read of the root range found no leader that serves it within its budget."
            }
            Self::Leaderless => {
                "Please reduce your request rate. A range's read or command found no leader that serves it within its budget."
            }
            Self::Unreadable => {
                "Please reduce your request rate. Fewer of a block's chunks could be read than rebuild it."
            }
            Self::Unplaced => {
                "Please reduce your request rate. No volume offered took a block's chunk."
            }
            Self::Patience => {
                "Please reduce your request rate. The operation's work was held past a client's patience."
            }
        }
    }
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
    Bytes(Bytes),
    /// The body ended whole, its signature's and its checksum's word held: the checksum an
    /// aws-chunked body's trailer carried, where it carried one (05 §3.2).
    End(Option<Checksum>),
    /// The body did not hold as it streamed: its signature, its digest or its framing, the status
    /// that says which; nothing of it commits.
    Refused(Status),
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
    /// A get's bytes, in order.
    Bytes(Bytes),
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
}

/// The listener's end: where it submits requests, and the waker of the node that takes them.
#[derive(Clone)]
pub struct Submitter {
    requests: Sender<Asked>,
    node: Waker,
}

/// A door for `capacity` requests waiting at once, past which a submission is refused at once
/// (`503 SlowDown`, node.md §4.5), for the node whose task `node` wakes.
pub fn doorway(capacity: usize, node: Waker) -> Result<(Doorway, Submitter), RtError> {
    let (requests, taken) = channel(capacity)?;
    Ok((
        Doorway {
            requests: taken,
            held: None,
        },
        Submitter { requests, node },
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
}
