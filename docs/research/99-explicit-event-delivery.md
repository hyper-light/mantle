# 99. Explicit event delivery: stepping the jobs an event concerns

Status: research note, 2026-11-20. For docs/design/node.md §2.9 step 6d: why the node's host steps
only the jobs an event names, and when a job may wait for one. Sources [1] and [2] were read on the
web on 2026-11-20 and are quoted where read.

## The question

The host held every operation in flight and stepped each at every turn of the node's task, though
most waited on something that had not moved: a peer's answer, its client's next piece of body, room
in its client's reply. What does a server pay for looking at every one of its waiters at each turn,
and what discipline lets a waiter be passed over without losing the event that would move it?

## Sources

[1] Linux man-pages, `epoll(7)`, https://man7.org/linux/man-pages/man7/epoll.7.html. The interest
list is "the set of file descriptors that the process has registered an interest in monitoring";
"The ready list is dynamically populated by the kernel as a result of I/O activity on those file
descriptors." Of its two modes, "edge-triggered mode delivers events only when changes occur on the
monitored file descriptor", and the man page's suggested use of it is "(1) with nonblocking file
descriptors; and" "(2) by waiting for an event only after read(2) or write(2) return EAGAIN."

[2] G. Banga, J. C. Mogul and P. Druschel, "A scalable and explicit event delivery mechanism for
UNIX", Proceedings of the USENIX Annual Technical Conference, Monterey, June 1999. Abstract: "The
select() mechanism works well for small-scale applications, but scales poorly as the number of file
descriptors increases." "Previous work has shown that while the traditional implementation of
select() can be improved, the poor scalability is inherent in the design. We present a new
event-delivery mechanism, which allows the application to register interest in one or more sources
of events, and to efficiently dequeue new events. We show that this mechanism, which requires only
minor changes to applications, performs independently of the number of file descriptors." §1:
"UNIX supports non-blocking operation for read() and write(), but a naive use of this mechanism, in
which the application polls each file descriptor to see if it might be usable, leads to excessive
overheads."

## What the design takes

- **A turn costs by its events, not its waiters** ([2]). A turn that steps every job is the naive
  poll [2] §1 names: its cost grows with the jobs in flight, thousands at 10,000 operations a second
  from 2,048 clients, however few moved. Each source that can move a job names it, and the turn
  steps those named, as an application dequeues only new events [2] and epoll returns its ready
  list alone [1].
- **A job waits for an event only once its step found nothing** ([1]). Edge-triggered delivery
  says only that something changed, so a waiter that stops before it drained what it was told of
  waits for good. A job rests only once its step found its body's channel empty or its reply's
  full, the host's `EAGAIN`, and its name is let go before the step that serves it, so a change
  before the step is seen by it and one after names it again.
- **A name with no room is a level, not a loss.** The names are held in a bounded queue, each
  request once until taken; one that finds no room sets a word that makes the next turn step every
  job of its door, as a level-triggered look at the whole interest list would, once.
