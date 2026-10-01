//! Tickets: how a caller hears its answer (mantle docs/design/node.md §1.3, note 32 §3.9).
//!
//! Every call into the log's owner carries a ticket, and the owner answers through it once. The
//! answer travels a reply port of the caller's own, a bounded channel that only the caller
//! receives on, so an answer unparks the one thread that waits for it and no other; a caller that
//! waits as a task instead gives a [`Waker`], which the owner wakes once, after the answer, so a
//! completion wakes only its submitter (research/26 §5.3). Nothing is shared but the channel the
//! answer moves through.
//!
//! A port is made once and reused. The final answer carries the port's sending end back to the
//! caller, which then holds both ends and keeps them for its next call, so a steady caller makes
//! no channel per call. Each thread keeps the ports it has finished with; it holds at most as many
//! as it once had calls out at a time, which the log's queue bounds for submissions and which is
//! one for a call that waits (`Log::view`, `Log::term`, ...).

use std::cell::{Cell, RefCell};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::task::Waker;

use crate::{Fetched, LogError, View};

/// Replies one ticket carries at most: its admission, for a submission, and its answer.
const REPLIES: usize = 2;

/// A reply through a ticket.
pub(crate) enum Reply {
    /// The submission holds its room in the queue; its answer follows once it is durable.
    Admitted,
    /// The call's answer, and the port's sending end, given back with it.
    Answer(Result<Answer, LogError>, SyncSender<Reply>),
}

/// What a call is answered.
pub(crate) enum Answer {
    /// A submission is durable.
    Durable,
    Groups(Vec<u128>),
    View(Option<View>),
    Term(u64),
    Entries(Fetched),
    Flushed(u64, u64),
    Fenced(bool),
}

/// A port, both ends.
type Port = (SyncSender<Reply>, Receiver<Reply>);

thread_local! {
    /// This thread's ports, free for its next call.
    static PORTS: RefCell<Vec<Port>> = const { RefCell::new(Vec::new()) };
}

/// A free port of this thread's, or a new one.
pub(crate) fn port() -> Port {
    PORTS
        .try_with(|ports| ports.try_borrow_mut().ok().and_then(|mut p| p.pop()))
        .ok()
        .flatten()
        .unwrap_or_else(|| sync_channel(REPLIES))
}

/// Keeps a port, answered and empty, for this thread's next call.
fn keep(port: Port) {
    let _ = PORTS.try_with(|ports| {
        if let Ok(mut ports) = ports.try_borrow_mut() {
            ports.push(port);
        }
    });
}

/// The owner's side of a call: where its replies go, and the waker to wake after its answer.
pub(crate) struct Ticket {
    reply: Option<SyncSender<Reply>>,
    waker: Option<Waker>,
}

impl Ticket {
    pub(crate) fn new(reply: SyncSender<Reply>, waker: Option<Waker>) -> Self {
        Self {
            reply: Some(reply),
            waker,
        }
    }

    /// Tells the caller its submission is admitted.
    pub(crate) fn admit(&self) {
        if let Some(reply) = &self.reply {
            // The port holds both replies, so this never finds it full; a caller that stopped
            // waiting has nothing to be told.
            let _ = reply.try_send(Reply::Admitted);
        }
    }

    /// Answers the call, once, and wakes its waker.
    pub(crate) fn answer(&mut self, answer: Result<Answer, LogError>) {
        if let Some(reply) = self.reply.take() {
            let back = reply.clone();
            let _ = reply.try_send(Reply::Answer(answer, back));
        }
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    /// Whether the call has been answered.
    pub(crate) fn answered(&self) -> bool {
        self.reply.is_none()
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        // Never answered: the log closed before it could be, and the caller hears so.
        if !self.answered() {
            self.answer(Err(LogError::Closed));
        }
    }
}

/// The caller's side of a call: its port's receiving end until the answer comes.
pub(crate) struct Waiting {
    receive: Option<Receiver<Reply>>,
    /// The port's sending end, once the answer brought it back.
    back: Cell<Option<SyncSender<Reply>>>,
    /// The answer was taken: a later look finds the call closed.
    taken: Cell<bool>,
}

impl Waiting {
    pub(crate) fn new(receive: Receiver<Reply>) -> Self {
        Self {
            receive: Some(receive),
            back: Cell::new(None),
            taken: Cell::new(false),
        }
    }

    /// Waits for the answer.
    pub(crate) fn wait(&self) -> Result<Answer, LogError> {
        let receive = self.receive.as_ref().ok_or(LogError::Closed)?;
        if self.taken.get() {
            return Err(LogError::Closed);
        }
        loop {
            match receive.recv() {
                Ok(Reply::Admitted) => {}
                Ok(Reply::Answer(answer, back)) => return self.took(answer, back),
                Err(_) => return Err(LogError::Closed),
            }
        }
    }

    /// The answer if it has come.
    pub(crate) fn poll(&self) -> Option<Result<Answer, LogError>> {
        let Some(receive) = self.receive.as_ref() else {
            return Some(Err(LogError::Closed));
        };
        if self.taken.get() {
            return Some(Err(LogError::Closed));
        }
        loop {
            match receive.try_recv() {
                Ok(Reply::Admitted) => {}
                Ok(Reply::Answer(answer, back)) => return Some(self.took(answer, back)),
                Err(TryRecvError::Empty) => return None,
                Err(TryRecvError::Disconnected) => return Some(Err(LogError::Closed)),
            }
        }
    }

    /// Waits for a submission's admission: `Ok` once admitted, its refusal otherwise.
    pub(crate) fn admitted(&self) -> Result<(), LogError> {
        let receive = self.receive.as_ref().ok_or(LogError::Closed)?;
        match receive.recv() {
            Ok(Reply::Admitted) => Ok(()),
            Ok(Reply::Answer(answer, back)) => self.took(answer, back).and(Err(LogError::Closed)),
            Err(_) => Err(LogError::Closed),
        }
    }

    fn took(
        &self,
        answer: Result<Answer, LogError>,
        back: SyncSender<Reply>,
    ) -> Result<Answer, LogError> {
        self.back.set(Some(back));
        self.taken.set(true);
        answer
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        // Answered: both ends are here and the port is empty, so it serves the next call.
        if let (Some(back), Some(receive)) = (self.back.take(), self.receive.take()) {
            keep((back, receive));
        }
    }
}

/// An update's answer, once it is durable or refused.
pub struct Pending(pub(crate) Waiting);

impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pending").finish_non_exhaustive()
    }
}

impl Pending {
    /// Waits until the update is durable, or refused.
    pub fn wait(&self) -> Result<(), LogError> {
        self.0.wait().and_then(durable)
    }

    /// The answer if it has come. An answer is given once; a look after it finds the call
    /// closed.
    pub fn poll(&self) -> Option<Result<(), LogError>> {
        self.0.poll().map(|a| a.and_then(durable))
    }
}

fn durable(answer: Answer) -> Result<(), LogError> {
    match answer {
        Answer::Durable => Ok(()),
        _ => Err(LogError::Closed),
    }
}

/// A fetch of entries, answered with the caller's reservation filled.
pub struct Fetching(pub(crate) Waiting);

impl std::fmt::Debug for Fetching {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fetching").finish_non_exhaustive()
    }
}

impl Fetching {
    /// Waits for the entries.
    pub fn wait(&self) -> Result<Fetched, LogError> {
        self.0.wait().and_then(fetched)
    }

    /// The entries if they have come.
    pub fn poll(&self) -> Option<Result<Fetched, LogError>> {
        self.0.poll().map(|a| a.and_then(fetched))
    }
}

fn fetched(answer: Answer) -> Result<Fetched, LogError> {
    match answer {
        Answer::Entries(f) => Ok(f),
        _ => Err(LogError::Closed),
    }
}
