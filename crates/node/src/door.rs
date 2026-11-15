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
    /// The lookup was not answered: a stopping node or a root range without a leader that can
    /// serve it (`SlowDown`), or the root range's rows failing (`Failed`).
    Refused(Status),
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
}

/// The node's end of its door: the requests the listener submits, and the waker that turns the
/// node to take them.
pub struct Doorway {
    requests: ChannelReceiver<Asked>,
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
    Ok((Doorway { requests: taken }, Submitter { requests, node }))
}

impl Doorway {
    /// The next request submitted, if one waits; none too once the listener is gone.
    pub fn take(&mut self) -> Option<Asked> {
        self.requests.try_recv().ok().flatten()
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
