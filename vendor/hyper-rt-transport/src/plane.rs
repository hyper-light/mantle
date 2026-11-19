//! The datagram plane's socket: its own, beside the endpoint's (mantle note 32 §3.5). Each datagram it
//! opens comes with when it arrived on the shard's clock, the kernel's receive stamp where the platform
//! gives one: what a node-pair heartbeat is judged by (hyper-raft docs/timing.md §2.4).
//!
//! **A receive costs what it receives** (mantle: vendor/UPSTREAM.md). The socket is asked only while
//! it may hold datagrams: as it is made, once its read readiness fires, and while its last receive left
//! some. Its readiness is hyper-rt's one-shot wait, which every driver reports while datagrams are
//! queued, not only as one arrives (epoll's `EPOLLONESHOT` without `EPOLLET` is level-triggered, and a
//! re-arm through `EPOLL_CTL_MOD` polls the socket again, epoll(7); kqueue's `EV_ADD` of `EVFILT_READ`
//! reports a socket whose receive buffer holds data, kqueue(2); an AFD poll completes at once on a
//! socket with data, as does the simulation fabric's wait). So a wait armed after a receive emptied the
//! socket fires for a datagram that came between the two, and one that came while the process was
//! stopped, its stamp the kernel's from its arrival. An empty socket costs no system call: before, each
//! poll of the receive asked the kernel, and on Linux made a whole batch's headers for it.
//! [`PlaneSocket::receive_ready`] asks the kernel whatever the readiness says, for a caller that must
//! know what was queued at a clock reading it took.

use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper_datagram::{Fence, Opened, PeerId, Plane, Refusal};
use hyper_rt::RtError;
use hyper_rt::readiness::Ready;
use hyper_rt::udp::{Arrival, Batched, Io, IoStats, Queued};

use crate::TURNS;

/// Carries a [`Plane`]'s datagrams on a batched UDP socket on a shard. The owner keeps the plane and
/// lends it to each call; the socket keeps only its bounded outbox and receive buffers.
#[derive(Debug)]
pub struct PlaneSocket {
    socket: Batched,
    readable: Option<Ready>,
    writable: Option<Ready>,
    /// Whether the socket may hold datagrams: as it is made, once its read readiness fired, and
    /// while its last receive left some. Otherwise it is asked nothing until its readiness fires.
    may_hold: bool,
}

impl PlaneSocket {
    /// A socket bound to `address`, with the kernel's receive stamps asked for.
    pub fn bind(address: SocketAddr, io: Io) -> Result<Self, RtError> {
        Ok(Self::new(Batched::bind(address, io, true)?))
    }

    /// The plane's socket on `socket`.
    pub fn new(socket: Batched) -> Self {
        Self {
            socket,
            readable: None,
            writable: None,
            may_hold: true,
        }
    }

    /// The address the socket is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr, RtError> {
        self.socket.local_addr()
    }

    /// What the socket has done.
    pub fn stats(&self) -> IoStats {
        self.socket.stats()
    }

    /// Seals what `plane` has queued and sends it, each peer's datagram to the address `route` names; a
    /// peer `route` does not know, or one the plane refuses to seal for, goes to `refused` (the plane
    /// retransmits nothing). Past the outbox's bound, with the socket full, a datagram is dropped and
    /// counted.
    pub fn flush(
        &mut self,
        plane: &mut Plane,
        mut route: impl FnMut(PeerId) -> Option<SocketAddr>,
        mut refused: impl FnMut(PeerId, Option<Refusal>),
    ) {
        let _ = self.socket.send();
        let socket = &mut self.socket;
        plane.flush(|peer, sealed| match sealed {
            Err(refusal) => refused(peer, Some(refusal)),
            Ok(datagram) => match route(peer) {
                None => refused(peer, None),
                Some(to) => {
                    if socket.slot().is_none() {
                        let _ = socket.send();
                    }
                    socket.queue(to, datagram);
                }
            },
        });
        let _ = self.socket.send();
    }

    /// Waits for datagrams and opens one batch with `plane` under `fence`, handing each result to
    /// `deliver` with its arrival; returns how many. Sends what the outbox holds when the socket takes it.
    /// Cancel-safe: a batch is taken and delivered without an await between.
    pub async fn receive(
        &mut self,
        plane: &mut Plane,
        fence: &dyn Fence,
        mut deliver: impl FnMut(Arrival, Result<Opened<'_>, Refusal>),
    ) -> Result<usize, RtError> {
        poll_fn(|context| self.poll_receive(context, plane, fence, &mut deliver)).await
    }

    /// Opens every datagram queued on the socket when it is called, without waiting and whatever its
    /// readiness says: the kernel is asked, so a datagram that arrived before the call, through a
    /// stop of the process too, is taken with its stamp. For a caller that judges what came by a
    /// clock reading it took before the call (a heartbeat stream polled at that reading), and at a
    /// start or after a hold. At most [`TURNS`] batches; returns how many datagrams.
    pub fn receive_ready(
        &mut self,
        plane: &mut Plane,
        fence: &dyn Fence,
        mut deliver: impl FnMut(Arrival, Result<Opened<'_>, Refusal>),
    ) -> Result<usize, RtError> {
        self.may_hold = true;
        let mut total = 0usize;
        for _ in 0..TURNS {
            let count = self.take(plane, fence, &mut deliver)?;
            total = total.saturating_add(count);
            if count == 0 || !self.may_hold {
                break;
            }
        }
        Ok(total)
    }

    /// Opens, without waiting, what the socket's readiness says may be queued: the socket is asked
    /// only while it may hold datagrams (its readiness fired since a receive emptied it, or its last
    /// receive left some), and its read readiness is armed for `context`'s task once it is empty, so
    /// an empty socket costs no system call. What came since the shard last read its driver waits
    /// for the readiness that tells of it. At most [`TURNS`] batches; returns how many datagrams.
    pub fn receive_readable(
        &mut self,
        context: &mut Context<'_>,
        plane: &mut Plane,
        fence: &dyn Fence,
        mut deliver: impl FnMut(Arrival, Result<Opened<'_>, Refusal>),
    ) -> Result<usize, RtError> {
        let mut total = 0usize;
        for _ in 0..TURNS {
            if !self.fired(context)? {
                break;
            }
            let count = self.take(plane, fence, &mut deliver)?;
            total = total.saturating_add(count);
            if count == 0 && self.may_hold {
                break;
            }
        }
        Ok(total)
    }

    /// Whether the socket's last receive emptied it: it found nothing, or took fewer than it offered
    /// room for, and it is asked again only once its readiness fires.
    pub fn emptied(&self) -> bool {
        !self.may_hold
    }

    /// One batch from the socket, opened with `plane` under `fence`; whether the socket may hold
    /// more is kept for the next receive.
    fn take(
        &mut self,
        plane: &mut Plane,
        fence: &dyn Fence,
        deliver: &mut impl FnMut(Arrival, Result<Opened<'_>, Refusal>),
    ) -> Result<usize, RtError> {
        let (count, ended) = self.socket.receive_batch(|arrival, datagram| {
            deliver(arrival, plane.open(datagram, fence));
        })?;
        self.may_hold = ended == Queued::More;
        Ok(count)
    }

    /// Whether the socket may hold datagrams: it may already, or its read readiness, armed for
    /// `context`'s task while it may not, fired.
    fn fired(&mut self, context: &mut Context<'_>) -> Result<bool, RtError> {
        if self.may_hold {
            return Ok(true);
        }
        let readable = self.readable.get_or_insert_with(|| self.socket.readable());
        if let Poll::Ready(outcome) = Pin::new(readable).poll(context) {
            self.readable = None;
            outcome?;
            self.may_hold = true;
        }
        Ok(self.may_hold)
    }

    fn poll_receive(
        &mut self,
        context: &mut Context<'_>,
        plane: &mut Plane,
        fence: &dyn Fence,
        deliver: &mut impl FnMut(Arrival, Result<Opened<'_>, Refusal>),
    ) -> Poll<Result<usize, RtError>> {
        for _ in 0..TURNS {
            if self.socket.pending() {
                let _ = self.socket.send();
            }
            match self.fired(context) {
                Ok(true) => match self.take(plane, fence, deliver) {
                    // Calls that handed nothing over (astray reports, datagrams passed over) and
                    // left more: the rest at the task's next poll, after the shard's other tasks.
                    Ok(0) if self.may_hold => {
                        context.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    // Emptied: its readiness is armed next.
                    Ok(0) => continue,
                    Ok(count) => return Poll::Ready(Ok(count)),
                    Err(error) => return Poll::Ready(Err(error)),
                },
                Ok(false) => {}
                Err(error) => return Poll::Ready(Err(error)),
            }
            if self.socket.pending() {
                let writable = self.writable.get_or_insert_with(|| self.socket.writable());
                if let Poll::Ready(outcome) = Pin::new(writable).poll(context) {
                    self.writable = None;
                    if let Err(error) = outcome {
                        return Poll::Ready(Err(error));
                    }
                    continue;
                }
            }
            return Poll::Pending;
        }
        context.waker().wake_by_ref();
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A task that awaits the plane's receive may be moved to another thread with its future, as
    /// `spawn_on` moves a task to its shard (hyper-rt's threaded runtime and its simulation alike):
    /// the future holds the owner's fence across its await, and a fence is `Sync`. Checked when the
    /// test compiles: the future is made and never polled.
    #[test]
    fn a_planes_receive_is_a_send_future() {
        fn send<F: Future + Send>(future: F) -> F {
            future
        }
        fn receive<'a>(
            socket: &'a mut PlaneSocket,
            plane: &'a mut Plane,
            fence: &'a dyn Fence,
        ) -> impl Future<Output = Result<usize, RtError>> + Send + 'a {
            send(socket.receive(plane, fence, |_, _| {}))
        }
        let _ = receive;
    }
}
