//! The S3 listener's connections spread over the node's shards (node.md §4.1). The listener
//! accepts on the node's shard, the first, which keeps the node's ranges, its log, its transport
//! and its QUIC door: one durable owner, one log writer, one UDP endpoint. It hands each connection
//! to the lane of another shard with the fewest connections open ([`least`]), passing over a shard
//! hyper-rt's watchdog sees held in one poll past its limit and one gone: a channel of that
//! shard's own, bounded at the connections the listener holds at once, whose taker, a task on that
//! shard, starts the connection there, so the connection's future is made on the shard that runs
//! it, as hyper-rt's `tcp::serve` hands a connection off. A lane that refuses one, its channel full
//! or its taker gone with its shard, hands it back whole, and the listener serves it on its own
//! shard: a spawn through the shard's control queue would drop it with the request, a queue whose
//! depth can be one.
//!
//! A lane counts its connections in a budget of its own: each handed connection's stream carries
//! one of its slots, given back as the stream drops with the connection's end, wherever that is.

use hyper_rt::RtError;
use hyper_rt::SlotHolder;
use hyper_rt::sync::{Sender, SyncError, channel};
use hyper_rt::tcp::{ConnectionBudget, ConnectionSlot};

/// A shard connections are handed to: the shard, the channel its taker reads, and the connections
/// open there.
pub(crate) struct Lane<T> {
    holder: SlotHolder,
    sender: Sender<T>,
    open: ConnectionBudget,
}

impl<T: Send + 'static> Lane<T> {
    /// A lane to the shard `holder` names for `capacity` connections, the most its channel holds
    /// and the most its count takes, whose taker there hands each to `start` on that shard. The
    /// taker ends when the lane goes. Refused when the channel or count cannot be made, or the
    /// shard takes no task now: gone, or its control queue full.
    pub(crate) fn new<S>(holder: SlotHolder, capacity: u64, mut start: S) -> Result<Self, RtError>
    where
        S: FnMut(T) + Send + 'static,
    {
        let held = usize::try_from(capacity).map_err(|_| RtError::Capacity {
            what: "a lane's connections",
            bound: usize::MAX,
        })?;
        let (sender, mut taken) = channel(held)?;
        let open = ConnectionBudget::new(capacity)?;
        let taker = async move {
            while let Ok(handed) = taken.recv().await {
                start(handed);
            }
        };
        hyper_rt::runtime::submit_to_holder(holder, taker)?;
        Ok(Self {
            holder,
            sender,
            open,
        })
    }
}

impl<T> Lane<T> {
    /// The connections open on the lane's shard.
    pub(crate) fn open(&self) -> u64 {
        self.open.open()
    }

    /// Whether the lane's shard takes a connection now: neither gone, nor held in one poll past its
    /// limit, as hyper-rt's watchdog sees it (`watch::stalled`), where a connection handed to it
    /// would wait for the poll to end before its first byte is read.
    pub(crate) fn takes(&self) -> bool {
        hyper_rt::watch::stalls(self.holder).is_some_and(|stalls| !stalls.stalled)
    }

    /// A connection's place in the lane's count, held by its stream and given back as the stream
    /// drops; none once the lane counts as many as it holds.
    pub(crate) fn place(&self) -> Option<ConnectionSlot> {
        self.open.try_take()
    }

    /// Sends `handed` to the lane's shard; handed back whole when its channel refuses it, full or
    /// its taker gone.
    pub(crate) fn hand(&self, handed: T) -> Result<(), T> {
        self.sender
            .try_send(handed)
            .map_err(|refused| match refused {
                SyncError::Closed(handed)
                | SyncError::Full(handed)
                | SyncError::NotOnShardThread(handed) => handed,
            })
    }
}

/// The lane with the fewest connections open, the first of those tied, of those whose shard takes a
/// connection now ([`Lane::takes`]); none where no lane's does, and the listener serves the
/// connection on its own shard, as it does one a lane refuses.
pub(crate) fn least<T>(lanes: &[Lane<T>]) -> Option<&Lane<T>> {
    lanes
        .iter()
        .filter(|lane| lane.takes())
        .min_by_key(|lane| lane.open())
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::{Read, Write};
    use std::sync::mpsc::sync_channel;

    use hyper_rt::runtime::{Runtime, RuntimeConfig};
    use hyper_rt::shard::ShardId;
    use hyper_rt::tcp::{OwnedStream, TcpStream};

    use super::*;

    /// A runtime of `shards` shards, each with `tasks` task slots.
    pub(crate) fn started(shards: u16, tasks: usize) -> Runtime {
        Runtime::start(&RuntimeConfig {
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

    /// A connected pair: a client's end and the accepted end as the socket a lane moves.
    fn connected() -> (std::net::TcpStream, OwnedStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        (client, OwnedStream::from(accepted))
    }

    /// A lane to `rt`'s shard `index` of `open` connections open, its taker unused: a lane a
    /// shard, as a control queue of one entry takes one taker at once.
    fn lane_of(rt: &Runtime, index: usize, open: u64) -> (Lane<()>, Vec<ConnectionSlot>) {
        let holder = rt.holder_of(rt.shard_ids()[index]).unwrap();
        let lane = Lane::new(holder, 8, |()| {}).unwrap();
        let held = (0..open).map(|_| lane.place().unwrap()).collect();
        (lane, held)
    }

    /// The listener hands each connection to the lane with the fewest open, the first of those
    /// tied; a connection's place given back, its lane counts one fewer.
    #[test]
    fn a_connection_goes_to_the_lane_with_the_fewest_open() {
        let rt = started(4, 1);
        assert!(least::<()>(&[]).is_none());
        let (a, _a) = lane_of(&rt, 1, 3);
        let (b, mut b_held) = lane_of(&rt, 2, 1);
        let (c, _c) = lane_of(&rt, 3, 2);
        let lanes = [a, b, c];
        // 3, 1, 2 open: the second.
        assert!(std::ptr::eq(least(&lanes).unwrap(), &lanes[1]));
        b_held.push(lanes[1].place().unwrap());
        // 3, 2, 2: tied, the first of those tied.
        assert_eq!(lanes[1].open(), 2);
        assert!(std::ptr::eq(least(&lanes).unwrap(), &lanes[1]));
        b_held.push(lanes[1].place().unwrap());
        // 3, 3, 2: the third.
        assert!(std::ptr::eq(least(&lanes).unwrap(), &lanes[2]));
        // 3, 0, 2: the second's places given back, the second again.
        drop(b_held);
        assert_eq!(lanes[1].open(), 0);
        assert!(std::ptr::eq(least(&lanes).unwrap(), &lanes[1]));
        drop(lanes);
        rt.shutdown().unwrap();
    }

    /// A runtime of `shards` shards whose watchdog's limit is short: a fixed quantum of a
    /// millisecond and a batch of one, a limit of two milliseconds (hyper-rt `watch`). Each shard
    /// holds a lane's taker and a wedged task, its control queue taking both at once.
    fn watched(shards: u16) -> Runtime {
        Runtime::start(&RuntimeConfig {
            shards,
            tasks_per_shard: 2,
            timers_per_shard: 1,
            interests_per_shard: hyper_rt::runtime::interests_for(2),
            ring_entries: 2,
            step_budget_ns: 1_000_000,
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

    /// Wedges the shard `holder` names in one poll, blocked inside a call until the sender it
    /// returns is dropped.
    fn wedge(holder: SlotHolder) -> std::sync::mpsc::SyncSender<()> {
        let (release, released) = sync_channel::<()>(1);
        hyper_rt::runtime::submit_to_holder(holder, async move {
            let _ = released.recv();
        })
        .unwrap();
        release
    }

    /// Waits until `test` holds: the watchdog's look comes every half limit, a millisecond, so ten
    /// seconds is far past it on a loaded host and only fails a test that would never hold.
    fn wait_for(what: &str, mut test: impl FnMut() -> bool) {
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !test() {
            assert!(std::time::Instant::now() < until, "waited for {what}");
            std::thread::yield_now();
        }
    }

    /// Placement passes over a lane whose shard the watchdog sees held in one poll past its limit,
    /// the fewest open among the rest taking the connection; with every lane's shard held, or
    /// gone, there is none, and the listener serves the connection on its own shard. A shard let
    /// go takes connections again.
    #[test]
    fn placement_passes_over_a_stalled_or_gone_shard() {
        let rt = watched(4);
        let holders: Vec<SlotHolder> = rt.shard_ids()[1..]
            .iter()
            .map(|shard| rt.holder_of(*shard).unwrap())
            .collect();
        let lane = |holder: SlotHolder, open: u64| {
            let lane = Lane::new(holder, 8, |()| {}).unwrap();
            let held: Vec<ConnectionSlot> = (0..open).map(|_| lane.place().unwrap()).collect();
            (lane, held)
        };
        let (a, _a) = lane(holders[0], 3);
        let (b, _b) = lane(holders[1], 1);
        let (c, _c) = lane(holders[2], 2);
        let lanes = [a, b, c];
        assert!(lanes.iter().all(Lane::takes));
        // 3, 1, 2 open: the second.
        assert!(std::ptr::eq(least(&lanes).unwrap(), &lanes[1]));

        // The second's shard held: the third, the fewest of the rest.
        let held_b = wedge(holders[1]);
        wait_for("the second shard stalled", || {
            hyper_rt::watch::stalled(holders[1])
        });
        assert!(!lanes[1].takes());
        assert!(std::ptr::eq(least(&lanes).unwrap(), &lanes[2]));

        // Every lane's shard held: none, the listener's own shard serves it.
        let held_a = wedge(holders[0]);
        let held_c = wedge(holders[2]);
        wait_for("every shard stalled", || {
            holders.iter().all(|h| hyper_rt::watch::stalled(*h))
        });
        assert!(least(&lanes).is_none());

        // Let go, the second takes connections again.
        drop((held_a, held_b, held_c));
        wait_for("no shard stalled", || {
            holders.iter().all(|h| !hyper_rt::watch::stalled(*h))
        });
        assert!(std::ptr::eq(least(&lanes).unwrap(), &lanes[1]));

        // Every lane's shard gone: none.
        rt.shutdown().unwrap();
        assert!(lanes.iter().all(|lane| !lane.takes()));
        assert!(least(&lanes).is_none());
    }

    /// A connection handed to a lane is adopted on the lane's shard, its task run there, and its
    /// place in the lane's count given back as its stream drops with the task's end.
    #[test]
    fn a_handed_connection_runs_on_the_lanes_shard() {
        // The taker, the connection's task and one spare.
        let rt = started(2, 3);
        let second = rt.shard_ids()[1];
        let (told, heard) = sync_channel::<(Option<ShardId>, Vec<u8>)>(1);
        let lane = Lane::new(
            rt.holder_of(second).unwrap(),
            4,
            move |(socket, placed): (OwnedStream, ConnectionSlot)| {
                let told = told.clone();
                let stream = TcpStream::from_parts(socket, Some(placed)).unwrap();
                hyper_rt::futures::spawn_detached(async move {
                    let mut read = [0u8; 5];
                    let mut at = 0;
                    while at < read.len() {
                        at += stream.read(&mut read[at..]).await.unwrap();
                    }
                    drop(stream);
                    told.try_send((hyper_rt::futures::shard_id(), read.to_vec()))
                        .unwrap();
                })
                .unwrap();
            },
        )
        .unwrap();
        let (mut client, socket) = connected();
        let placed = lane.place().unwrap();
        assert_eq!(lane.open(), 1);
        assert!(lane.hand((socket, placed)).is_ok());
        client.write_all(b"hello").unwrap();
        let (shard, read) = heard.recv().unwrap();
        assert_eq!(shard, Some(second));
        assert_eq!(read, b"hello");
        assert_eq!(lane.open(), 0);
        drop(lane);
        rt.shutdown().unwrap();
    }

    /// A lane whose taker is gone, its shard stopped, hands the connection back whole: its socket
    /// still connected, for the listener to serve on its own shard.
    #[test]
    fn a_lane_that_refuses_hands_the_connection_back_whole() {
        let rt = started(2, 2);
        let lane = Lane::new(
            rt.holder_of(rt.shard_ids()[1]).unwrap(),
            4,
            |_: (OwnedStream, ConnectionSlot)| {},
        )
        .unwrap();
        rt.shutdown().unwrap();
        let (mut client, socket) = connected();
        let placed = lane.place().unwrap();
        let (socket, placed) = lane.hand((socket, placed)).unwrap_err();
        drop(placed);
        assert_eq!(lane.open(), 0);
        let mut kept = std::net::TcpStream::from(socket);
        client.write_all(b"kept").unwrap();
        let mut read = [0u8; 4];
        kept.read_exact(&mut read).unwrap();
        assert_eq!(&read, b"kept");
        kept.write_all(b"back").unwrap();
        client.read_exact(&mut read).unwrap();
        assert_eq!(&read, b"back");
    }
}
