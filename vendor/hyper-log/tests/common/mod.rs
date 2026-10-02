//! Simulated devices a test holds the writer at, shared by the log's tests.
//!
//! A log owns its file, on the thread that does its I/O, so a test does not share the device with
//! it: each wrapper here is moved into the log, and the test keeps the other end of a channel to
//! it, over which it lets flushes and reads through and hears when one is held. A test reaches the file
//! itself through `Log::with_file` while the log runs, and takes it back with `Log::close`.
#![allow(
    clippy::unwrap_used,
    clippy::arithmetic_side_effects,
    dead_code,
    unreachable_pub
)]

use std::cell::Cell;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::task::Waker;

use hyper_block::DiskError;
use hyper_block::block::BlockFile;
use hyper_block::buf::Alignment;
use hyper_block::sim::{Fault, SimFile};

/// Commands and events a test exchanges with a held device at most at once: a test sends a few
/// commands between the device's flushes, and hears of each flush held once.
const CHANNEL: usize = 1 << 10;

/// What a test tells a held device.
enum Command {
    /// Holds every flush from the next on.
    Hold,
    /// Lets one more held flush through.
    Allow,
    /// Lets every flush through from here on.
    Release,
    /// Lets every read through from here on.
    Untrap,
    /// Holds reads overlapping `[from, to)` until released.
    Trap(u64, u64),
    /// Arms a fault on the file, between two of the device's operations.
    Inject(Fault),
}

/// The device's side of a hold: the file, and the commands that say what to hold.
pub struct Held {
    file: SimFile,
    commands: Receiver<Command>,
    events: SyncSender<usize>,
    holding: Cell<bool>,
    /// Flushes that have arrived, and how many of them may complete.
    arrived: Cell<u64>,
    allowed: Cell<u64>,
    trap: Cell<Option<(u64, u64)>>,
}

/// What a held device tells the test, on the channel that also carries the tags of the wakers
/// `Holder::waker` makes: a flush held, a read held, or `TOLD` plus a waker's tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    Flush,
    Read,
}

const FLUSH: usize = 0;
const READ: usize = 1;
/// The first number a waker's tag is told as.
const TOLD: usize = 2;

/// The test's side of a hold.
pub struct Holder {
    commands: SyncSender<Command>,
    events: Receiver<usize>,
    /// The device's end of `events`, kept for the wakers a test makes; `None` unless asked for,
    /// so that a test whose log has gone hears the channel close.
    tell: Option<SyncSender<usize>>,
    /// Events heard and not yet waited for.
    flushes: Cell<u64>,
    reads: Cell<u64>,
    /// One more than the greatest waker tag told, 0 if none.
    told: Cell<usize>,
}

/// A simulated file whose flushes and reads a test can hold, and the test's side of it.
pub fn held(file: SimFile) -> (Held, Holder) {
    let (device, mut holder) = held_telling(file);
    holder.tell = None;
    (device, holder)
}

/// `held`, whose holder also makes wakers that tell it when woken (`Holder::waker`).
pub fn held_telling(file: SimFile) -> (Held, Holder) {
    let (commands, receive) = sync_channel(CHANNEL);
    let (events, heard) = sync_channel(CHANNEL);
    let tell = Some(events.clone());
    (
        Held {
            file,
            commands: receive,
            events,
            holding: Cell::new(false),
            arrived: Cell::new(0),
            allowed: Cell::new(0),
            trap: Cell::new(None),
        },
        Holder {
            commands,
            events: heard,
            tell,
            flushes: Cell::new(0),
            reads: Cell::new(0),
            told: Cell::new(0),
        },
    )
}

impl Held {
    /// The simulated file under the hold.
    pub fn file(&self) -> &SimFile {
        &self.file
    }

    pub fn into_inner(self) -> SimFile {
        self.file
    }

    fn apply(&self, command: Command) {
        match command {
            Command::Hold => {
                self.holding.set(true);
                // Every flush before this one was let through.
                self.allowed.set(self.arrived.get().saturating_sub(1));
            }
            Command::Allow => self.allowed.set(self.allowed.get() + 1),
            Command::Release => self.holding.set(false),
            Command::Untrap => self.trap.set(None),
            Command::Trap(from, to) => self.trap.set(Some((from, to))),
            Command::Inject(fault) => self.file.inject(fault).unwrap(),
        }
    }

    /// Applies what the test has said so far.
    fn hear(&self) {
        while let Ok(command) = self.commands.try_recv() {
            self.apply(command);
        }
    }

    /// Waits for the test's next command; a test that has gone lets everything through.
    fn wait(&self) {
        match self.commands.recv() {
            Ok(command) => self.apply(command),
            Err(_) => {
                self.holding.set(false);
                self.trap.set(None);
            }
        }
    }
}

impl BlockFile for Held {
    fn alignment(&self) -> Alignment {
        self.file.alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        self.file.len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        self.hear();
        let end = offset + buf.len() as u64;
        let trapped = |trap: Option<(u64, u64)>| trap.is_some_and(|(a, b)| offset < b && a < end);
        if trapped(self.trap.get()) {
            let _ = self.events.send(READ);
            while trapped(self.trap.get()) {
                self.wait();
            }
        }
        self.file.read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        self.file.write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        self.arrived.set(self.arrived.get() + 1);
        self.hear();
        let held = || self.holding.get() && self.arrived.get() > self.allowed.get();
        if held() {
            let _ = self.events.send(FLUSH);
            while held() {
                self.wait();
            }
        }
        self.file.sync_data()
    }
}

impl Holder {
    /// Holds every flush from here on.
    pub fn hold(&self) {
        self.commands.send(Command::Hold).unwrap();
    }

    /// Waits until the writer is held in a flush.
    pub fn held(&self) {
        self.wait_for(Event::Flush);
    }

    /// Lets one more flush through before the next is held.
    pub fn allow(&self) {
        self.commands.send(Command::Allow).unwrap();
    }

    /// Arms a fault on the file between two of the device's operations, even while the device
    /// is held in one.
    pub fn inject(&self, fault: Fault) {
        self.commands.send(Command::Inject(fault)).unwrap();
    }

    /// Lets the held flush complete and waits until the writer is held in its next one.
    pub fn step(&self) {
        self.commands.send(Command::Allow).unwrap();
        self.held();
    }

    /// Lets every flush through from here on.
    pub fn release(&self) {
        let _ = self.commands.send(Command::Release);
    }

    /// Lets every read through from here on.
    pub fn untrap(&self) {
        let _ = self.commands.send(Command::Untrap);
    }

    /// A guard that lets everything through when dropped, as a failing test's unwinding drops
    /// it too, so a log closed after it never waits on a device held for good.
    pub fn released(&self) -> Released {
        Released(self.commands.clone())
    }

    /// Holds reads overlapping `len` bytes from `from` until released.
    pub fn trap(&self, from: u64, len: u64) {
        self.commands.send(Command::Trap(from, from + len)).unwrap();
    }

    /// Waits until a read is held.
    pub fn read_held(&self) {
        self.wait_for(Event::Read);
    }

    /// A waker that tells this holder `tag` when woken. Only a holder from `held_telling`
    /// makes one.
    pub fn waker(&self, tag: usize) -> Waker {
        let tell = self.tell.clone().unwrap();
        hyper_measure::wake::waker(TOLD + tag, tell).0
    }

    /// Waits until the writer is held in a flush (`true`) or the waker of `tag` is woken
    /// (`false`), whichever the holder hears of first.
    pub fn held_or_told(&self, tag: usize) -> bool {
        loop {
            self.listen();
            if self.flushes.get() > 0 {
                self.flushes.set(self.flushes.get() - 1);
                return true;
            }
            if self.told.get() > tag {
                return false;
            }
            self.note(self.events.recv().unwrap());
        }
    }

    /// Forgets every event heard or waiting: the holds and wakes of what the test has finished
    /// with. Every one the device sent before its last flush let through is waiting by then.
    pub fn settle(&self) {
        self.listen();
        self.flushes.set(0);
        self.reads.set(0);
    }

    fn note(&self, event: usize) {
        match event {
            FLUSH => self.flushes.set(self.flushes.get() + 1),
            READ => self.reads.set(self.reads.get() + 1),
            tag => self.told.set(self.told.get().max(tag - TOLD + 1)),
        }
    }

    fn listen(&self) {
        loop {
            match self.events.try_recv() {
                Ok(event) => self.note(event),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            }
        }
    }

    fn wait_for(&self, event: Event) {
        let count = match event {
            Event::Flush => &self.flushes,
            Event::Read => &self.reads,
        };
        loop {
            self.listen();
            if count.get() > 0 {
                count.set(count.get() - 1);
                return;
            }
            self.note(self.events.recv().unwrap());
        }
    }
}

/// Lets a held device's flushes and reads through when dropped.
pub struct Released(SyncSender<Command>);

impl Drop for Released {
    fn drop(&mut self) {
        let _ = self.0.send(Command::Release);
        let _ = self.0.send(Command::Untrap);
    }
}
