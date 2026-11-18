//! Real processes run as `mantle serve` (docs/design/membership.md): a cell whose members come and
//! go. A cell of one made by `mantle init` grows by grants (`mantle init grant`) and joins, each
//! node added to every range as a learner and made a voter once caught up; nodes are held still
//! (`SIGSTOP`) and killed (`SIGKILL`) for long stretches and come back; a node joins while another
//! is away; and nodes leave in order, their votes taken out of every range and their volumes out
//! of placement. A steady load of writes and reads runs throughout through both doors, the S3
//! listener signed with SigV4 and the native protocol, and after every step every object the cell
//! acknowledged reads back byte for byte through both. Each wait is on a fact a process says
//! (`enlisted`, `config`, `standing`, `leaving`, `left`), and the watch fails a cell that goes
//! quiet past its nodes' stated patience.
//!
//! A binary of its own (crates/mantle/tests/support/cell.rs's harness): a process states its
//! message bounds once, and these nodes derive theirs.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

#[allow(dead_code, unused_imports)]
#[path = "support/cell.rs"]
mod cell;
#[allow(dead_code)]
#[path = "../../node/tests/support/history.rs"]
mod history;
#[path = "support/hold.rs"]
mod hold;
#[allow(dead_code)]
#[path = "../../range/tests/support/linear.rs"]
mod linear;
#[allow(dead_code)]
#[path = "../../node/tests/support/relay.rs"]
mod relay;
#[allow(dead_code)]
#[path = "support/s3.rs"]
mod s3;
#[allow(dead_code)]
#[path = "support/s3_watched.rs"]
mod s3_watched;
use cell::*;
use std::io::Read as _;
use std::net::TcpStream;

/// The bucket every object of the load is put in.
const BUCKET: &str = "b";
/// Each object's bytes: past a datagram's payload and a volume's checksum block, so each crosses
/// the transfer class and spans checksum blocks, and small enough that every object of a run reads
/// back through both doors at every step.
const OBJECT_BYTES: usize = 5_000;

/// A node the load asks, as the test tells it: its id, its certificate, where its QUIC endpoint
/// and its S3 listener listen.
#[derive(Clone)]
struct Target {
    id: u64,
    certificate: Vec<u8>,
    quic: SocketAddr,
    s3: SocketAddr,
}

/// What the test tells the load: the nodes to ask now, numbered, or to end.
enum Lead {
    Nodes(u64, Vec<Target>),
    End,
}

/// The door an object went in by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Door {
    Native,
    S3,
}

/// What the load tells the test: that a round over the nodes it was told last began, by their
/// number; an object the cell acknowledged, by its number and its door; a read whose bytes were not
/// its object's, which fails the test; and the load's end.
enum Said {
    Led(u64),
    Acked(u64, Door),
    Wrong(String),
    Ended,
}

/// What the load needs to make its native client and sign its S3 requests: the object client's
/// identity, the cell's authority, the access key and the region.
#[derive(Clone)]
struct Identity {
    id: u64,
    certificate: Vec<u8>,
    key: Vec<u8>,
    authority: Vec<u8>,
    access: s3::Key,
    region: String,
}

/// A wait's answer: the fact it waits on holds.
fn holds(fact: bool) -> Option<()> {
    if fact { Some(()) } else { None }
}

/// Object `n`'s key.
fn key_of(n: u64) -> String {
    format!("o{n}")
}

/// The native client's settings over `targets`, as the harness's object clients are made.
fn client_settings(who: &Identity, targets: &[Target]) -> ClientSettings {
    let messages = served_bounds();
    let least = mantle_protocol::bounds::least_transport(&mantle_protocol::bounds::Stated {
        peers: targets.len(),
        replicas: 0,
        clients: 0,
        pool: 0,
        ranges: 1,
        inflight: 0,
        lanes: 1,
        transfers: 0,
        asked: 0,
        transport_bytes: 0,
        messages,
        max_head: mantle_node::node::MAX_HEAD,
    })
    .unwrap();
    ClientSettings {
        id: who.id,
        bind: "127.0.0.1:0".parse().unwrap(),
        credentials: Credentials {
            chain: vec![CertificateDer::from(who.certificate.clone())],
            key: PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(who.key.clone())),
            roots: vec![CertificateDer::from(who.authority.clone())],
        },
        nodes: targets
            .iter()
            .map(|t| Peer {
                id: t.id,
                role: Role::Node,
                certificate: t.certificate.clone(),
            })
            .collect(),
        role: Role::Client,
        messages,
        transport_bytes: u64::try_from(targets.len().max(1) * 3 * hyper_transport::RECEIVE_CHUNK)
            .unwrap()
            .max(least),
        seed: None,
    }
}

/// One S3 request to the listener at `at`, signed with SigV4: its status and body, or none where
/// the connection failed (a node killed). No deadline of its own: a node held still answers once it
/// goes on, and the test waits on the load under the cell's watch (`lead`, `acknowledged`,
/// `Steady::end`).
fn s3_request(
    who: &Identity,
    at: SocketAddr,
    method: &str,
    target: &str,
    body: &[u8],
) -> Option<(u16, Vec<u8>)> {
    let signer = s3::Client::new(at, who.access.clone(), &who.region);
    let payload = mantle_s3::sigv4::hex(&mantle_s3::crypto::sha256(body).unwrap());
    let length = body.len().to_string();
    let mut headers: Vec<(&str, &str)> = vec![("connection", "close")];
    if method == "PUT" {
        headers.push(("content-length", &length));
    }
    let head = signer.head(method, target, &headers, &payload);
    let mut stream = TcpStream::connect(at).ok()?;
    stream.write_all(head.as_bytes()).ok()?;
    stream.write_all(body).ok()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).ok()?;
    let end = response.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&response[..end]).into_owned();
    let status: u16 = head.split_whitespace().nth(1)?.parse().ok()?;
    let length: usize = head
        .lines()
        .find_map(|l| {
            let (name, value) = l.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())?
        })
        .unwrap_or(0);
    let body = response.get(end + 4..end + 4 + length)?.to_vec();
    Some((status, body))
}

/// The load: puts of new objects, every other one by each door, each read back once through the
/// other door, through the nodes the test names in turn, until it is told to end. An object counts
/// once the cell acknowledged it: a native put answered `Done`, an S3 put answered 200. A put the
/// cell refused or whose answer was lost is acknowledged by nothing and never read; a read through
/// a node that is gone is no failure; a read that answers other bytes than its object's fails the
/// test.
fn load(who: Identity, leads: Receiver<Lead>, tell: Sender<Said>) {
    let (mut led, mut targets): (u64, Vec<Target>) = match leads.recv() {
        Ok(Lead::Nodes(led, targets)) => (led, targets),
        _ => return,
    };
    let mut n = 0u64;
    let mut runtime = client_runtime(usize::try_from(cell::MOST_NODES).unwrap() + 1);
    loop {
        loop {
            match leads.try_recv() {
                Ok(Lead::Nodes(now, nodes)) => (led, targets) = (now, nodes),
                Ok(Lead::End) | Err(TryRecvError::Disconnected) => {
                    let _ = tell.send(Said::Ended);
                    return;
                }
                Err(TryRecvError::Empty) => break,
            }
        }
        if tell.send(Said::Led(led)).is_err() {
            return;
        }
        // A round over the nodes named now: each node put through once by each door, its objects
        // numbered from `first` in turn.
        let (round, who_now, tell_now, first) = (targets.clone(), who.clone(), tell.clone(), n);
        n += 2 * targets.len() as u64;
        let acked = runtime
            .block_on(async move { round_of(&who_now, &round, first, &tell_now).await })
            .unwrap_or_default();
        for (n, door) in acked {
            if tell.send(Said::Acked(n, door)).is_err() {
                return;
            }
        }
    }
}

/// One round of the load over `round`: each node put through once by each door, the objects
/// numbered from `first` in turn, each acknowledged put read back once through the other door by
/// the next node. The objects the cell acknowledged, each with its door.
async fn round_of(
    who: &Identity,
    round: &[Target],
    first: u64,
    tell: &Sender<Said>,
) -> Vec<(u64, Door)> {
    let mut acked: Vec<(u64, Door)> = Vec::new();
    let Ok(mut client) = Client::open(client_settings(who, round)).await else {
        return acked;
    };
    for t in round {
        let _ = client.learn(t.id, t.quic);
    }
    let mut n = first;
    for (at, t) in round.iter().enumerate() {
        for door in [Door::Native, Door::S3] {
            n += 1;
            let bytes = object_bytes(n, OBJECT_BYTES);
            let key = key_of(n);
            let put = match door {
                Door::Native => {
                    let op = put_op(BUCKET, &key, u128::from(n), bytes.len() as u64);
                    matches!(
                        client.object(t.id, &op, &bytes, u64::MAX).await,
                        Ok(ObjectOutcome::Replied { reply, .. }) if reply.status == ObjectStatus::Done
                    )
                }
                Door::S3 => matches!(
                    s3_request(who, t.s3, "PUT", &format!("/{BUCKET}/{key}"), &bytes),
                    Some((200, _))
                ),
            };
            if !put {
                continue;
            }
            acked.push((n, door));
            // Read back once through the other door, by the next node.
            let other = &round[(at + 1) % round.len()];
            let read = match door {
                Door::S3 => match client
                    .object(other.id, &get_op(BUCKET, &key, None), &[], u64::MAX)
                    .await
                {
                    Ok(ObjectOutcome::Replied { reply, bytes })
                        if reply.status == ObjectStatus::Done =>
                    {
                        Some(bytes)
                    }
                    _ => None,
                },
                Door::Native => {
                    match s3_request(who, other.s3, "GET", &format!("/{BUCKET}/{key}"), &[]) {
                        Some((200, body)) => Some(body),
                        _ => None,
                    }
                }
            };
            if let Some(read) = read
                && read != bytes
            {
                let why = format!(
                    "{key}, put by {door:?} through {}, read {} bytes through {} other than its own",
                    t.id,
                    read.len(),
                    other.id
                );
                let _ = tell.send(Said::Wrong(why));
            }
        }
    }
    acked
}

/// The running load: where it is told what to ask and where it tells what the cell acknowledged,
/// the number of the nodes it was told last and of those its latest round asks, and every object
/// acknowledged so far.
struct Steady {
    leads: Sender<Lead>,
    told: Receiver<Said>,
    told_last: u64,
    asking: u64,
    acked: BTreeMap<u64, Door>,
    thread: Option<JoinHandle<()>>,
}

impl Steady {
    fn start(who: Identity) -> Self {
        let (leads, heard) = channel();
        let (tell, told) = channel();
        let thread = std::thread::spawn(move || load(who, heard, tell));
        Self {
            leads,
            told,
            told_last: 0,
            asking: 0,
            acked: BTreeMap::new(),
            thread: Some(thread),
        }
    }

    /// Takes what the load told, failing at a read of other bytes than its object's.
    fn take(&mut self) {
        while let Ok(said) = self.told.try_recv() {
            match said {
                Said::Led(led) => self.asking = led,
                Said::Acked(n, door) => {
                    self.acked.insert(n, door);
                }
                Said::Wrong(why) => panic!("the load read wrong bytes: {why}"),
                Said::Ended => {}
            }
        }
    }

    /// Ends the load once its round under way is done, waited on under the cell's watch.
    #[track_caller]
    fn end(&mut self, cell: &mut Cell) {
        let _ = self.leads.send(Lead::End);
        if let Some(thread) = self.thread.take() {
            cell.watched("the load's last round", thread);
        }
        self.take();
    }
}

/// The nodes `ids` as the load asks them: each running node's certificate and where its QUIC
/// endpoint and its S3 listener listen.
fn targets(cell: &mut Cell, ids: &[u64]) -> Vec<Target> {
    ids.iter()
        .map(|id| {
            let s3 = cell.until(|c| c.s3_bound.get(id).map(|(at, _)| *at));
            Target {
                id: *id,
                certificate: cell.pki.nodes[id].2.clone(),
                quic: cell.running[id].bound.unwrap().0,
                s3,
            }
        })
        .collect()
}

/// Tells the load to ask `ids` from now on, and waits until a round of it does: no request of the
/// load is out to a node the test then holds still or kills.
fn lead(cell: &mut Cell, load: &mut Steady, ids: &[u64]) {
    let targets = targets(cell, ids);
    load.told_last += 1;
    let led = load.told_last;
    load.leads.send(Lead::Nodes(led, targets)).unwrap();
    // The load's word comes on its own channel: the cell's watch hears the nodes move as its puts
    // commit, and the load is taken at each look. Its round under way ends first, as its objects
    // are acknowledged: that round's objects are the count's ceiling.
    let acked = std::cell::Cell::new(load.acked.len() as u64);
    cell.until_by(Progress::Counted(&acked), |_| {
        load.take();
        acked.set(load.acked.len() as u64);
        holds(load.asking >= led)
    });
}

/// Waits until the load has had the cell acknowledge `more` objects past those it had.
fn acknowledged(cell: &mut Cell, load: &mut Steady, more: usize) {
    load.take();
    let wanted = load.acked.len() + more;
    let acked = std::cell::Cell::new(load.acked.len() as u64);
    cell.until_by(Progress::Counted(&acked), |_| {
        load.take();
        acked.set(load.acked.len() as u64);
        holds(load.acked.len() >= wanted)
    });
}

/// Every object the cell acknowledged so far reads back byte for byte through both doors: through
/// the native protocol of one of `through`, and S3 of another, by turns.
fn every_object_reads_back(cell: &mut Cell, load: &mut Steady, through: &[u64], step: &str) {
    load.take();
    let numbers: Vec<u64> = load.acked.keys().copied().collect();
    assert!(!numbers.is_empty(), "{step}: no object acknowledged");
    let gets: Vec<(u64, Op, Vec<u8>)> = numbers
        .iter()
        .enumerate()
        .map(|(at, n)| {
            (
                through[at % through.len()],
                get_op(BUCKET, &key_of(*n), None),
                Vec::new(),
            )
        })
        .collect();
    let read = cell.objects(gets);
    for (at, (outcome, n)) in read.iter().zip(&numbers).enumerate() {
        let node = through[at % through.len()];
        // A read refused or lost says the node it asked, and what the cell said last, with what it
        // said and was not yet taken: each range's status at each node, each node's links and its
        // record of the others; each node's longest poll and how long ago it said it; each node's
        // latest stall on each shard and how long ago it said it; and how long since each node's
        // last status moved, as a poll still holding a node says nothing.
        if !matches!(outcome, ObjectOutcome::Replied { reply, .. } if reply.status == ObjectStatus::Done)
        {
            cell.take_heard();
            let polls: BTreeMap<u64, (Duration, Duration)> = cell
                .polls
                .iter()
                .map(|(id, (heard, longest))| (*id, (*longest, heard.elapsed())))
                .collect();
            let stalls: BTreeMap<(u64, u64), (Stall, Duration)> = cell
                .stalls
                .iter()
                .map(|(at, (heard, stall))| (*at, (stall.clone(), heard.elapsed())))
                .collect();
            let since: BTreeMap<u64, Duration> = cell
                .log
                .iter()
                .map(|(id, (heard, ..))| (*id, heard.elapsed()))
                .collect();
            panic!(
                "{step}: {} natively through node {node}, read {at} of {}: {outcome:?}; status \
                 {:?}; links {:?}; standings {:?}; longest polls, each said how long ago \
                 {polls:?}; stalls by node and shard, each said how long ago {stalls:?}; since \
                 each node's last status {since:?}",
                key_of(*n),
                numbers.len(),
                cell.status,
                cell.links,
                cell.standings
            );
        }
        assert_eq!(
            done(
                outcome,
                &format!("{step}: {} natively through node {node}", key_of(*n))
            ),
            object_bytes(*n, OBJECT_BYTES),
            "{step}: {} read back natively through node {node} other than it was put by {:?}",
            key_of(*n),
            load.acked[n]
        );
    }
    let access = s3::Key::of(&cell.out());
    for (at, n) in numbers.iter().enumerate() {
        let node = through[(at + 1) % through.len()];
        let (listen, region, _) = cell.s3[&node].clone();
        let at = cell.s3_bound.get(&node).map_or(listen, |(at, _)| *at);
        let mut client = s3::Client::new(at, access.clone(), &region);
        let response = client.request(cell, "GET", &format!("/{BUCKET}/{}", key_of(*n)), &[], &[]);
        assert_eq!(
            response.status,
            200,
            "{step}: {} through S3 of node {node}: {}",
            key_of(*n),
            response.text()
        );
        assert_eq!(
            response.body,
            object_bytes(*n, OBJECT_BYTES),
            "{step}: {} read back through S3 of node {node} other than it was put by {:?}",
            key_of(*n),
            load.acked[n]
        );
    }
    let native = load
        .acked
        .values()
        .filter(|door| matches!(door, Door::Native))
        .count();
    eprintln!(
        "{step}: {} objects read back byte for byte through both doors ({native} put natively, {} \
         through S3), through {} nodes",
        numbers.len(),
        numbers.len() - native,
        through.len()
    );
}

/// `mantle init`'s arguments for a cell of one under `root`, stating bounds for the five nodes it
/// grows to: its founder with a volume of its own.
fn one_founder(root: &Path) -> Vec<String> {
    let dir = root.join("v1");
    std::fs::create_dir(&dir).unwrap();
    vec![
        "--node".into(),
        format!(
            "quic=127.0.0.1:0,plane=127.0.0.1:0,domain=d1,volume={}:256M",
            dir.display()
        ),
        "--max-nodes".into(),
        cell::MOST_NODES.to_string(),
        "--max-volumes".into(),
        cell::MOST_NODES.to_string(),
    ]
}

/// A node granted to the cell, with a volume of its own under the test's root in a domain of its
/// own, started, told where the running nodes listen, and enlisted under its grant.
fn joins(cell: &mut Cell, n: u64) -> u64 {
    let dir = cell._root.path().join(format!("v{n}"));
    std::fs::create_dir(&dir).unwrap();
    let id = cell.grant(&format!(
        "quic=127.0.0.1:0,plane=127.0.0.1:0,domain=d{n},volume={}:256M",
        dir.display()
    ));
    cell.start(id);
    cell.introduce();
    cell.enlisted(id);
    id
}

/// Whether `id` votes in a configuration.
fn votes(voters: &[u64], id: u64) -> bool {
    voters.contains(&id)
}

/// A cell of one grows to five under load, each joining node a learner and then a voter of every
/// range; one node is held still for a long stretch and goes on, another joins while a third is
/// held still, one is killed and started again; then three leave in order, their votes out of every
/// range, placement leaving their volumes; and after every step every object acknowledged reads
/// back byte for byte through both doors. The load runs throughout; while a member is away a block
/// whose scheme the cell's shape needs it for is refused, never written short (CLAUDE.md §6), so
/// the test waits for objects acknowledged only with every member present.
#[test]
fn a_cell_grows_from_one_to_five_and_back_to_two_voters_under_load() {
    let mut cell = Cell::made(one_founder);
    let founder = *cell.dirs.keys().next().unwrap();
    cell.enlisted(founder);
    let made = cell.objects(vec![(
        founder,
        Op::Create {
            bucket: BUCKET.into(),
            id: 1,
        },
        Vec::new(),
    )]);
    done(&made[0], "the bucket");
    let access = s3::Key::of(&cell.out());
    let region = cell.s3[&founder].1.clone();
    let (certificate, key) = cell.pki.objects[&object_client(CLIENT + 1)].clone();
    let mut load = Steady::start(Identity {
        id: object_client(CLIENT + 1),
        certificate,
        key,
        authority: cell.pki.authority_der.clone(),
        access,
        region,
    });
    let mut members = vec![founder];
    lead(&mut cell, &mut load, &members);
    acknowledged(&mut cell, &mut load, 4);
    every_object_reads_back(&mut cell, &mut load, &members, "a cell of one");

    // Two and three: each joins as a learner and is a voter once caught up.
    for n in 2..=3 {
        let id = joins(&mut cell, n);
        members.push(id);
        let all = members.clone();
        cell.configured(&members, |voters, _| all.iter().all(|m| votes(voters, *m)));
        lead(&mut cell, &mut load, &members);
        acknowledged(&mut cell, &mut load, 4);
        every_object_reads_back(&mut cell, &mut load, &members, &format!("{n} nodes"));
    }

    // The second node held still for a long stretch: the others serve on, and it goes on.
    let held = members[1];
    let others: Vec<u64> = members.iter().copied().filter(|m| *m != held).collect();
    lead(&mut cell, &mut load, &others);
    hold::still(cell.running[&held].child.id());
    // Its absence recorded by the root range's leader, from its liveness stream.
    cell.until(|c| {
        holds(others.iter().all(|o| {
            c.standings
                .get(&(*o, held))
                .is_some_and(|(_, since)| *since != 0)
        }))
    });
    hold::go_on(cell.running[&held].child.id());
    // Back: its return recorded.
    cell.until(|c| {
        holds(members.iter().all(|o| {
            c.standings
                .get(&(*o, held))
                .is_some_and(|(_, since)| *since == 0)
        }))
    });
    lead(&mut cell, &mut load, &members);
    acknowledged(&mut cell, &mut load, 4);
    every_object_reads_back(&mut cell, &mut load, &members, "after a node held still");

    // The fourth joins while the third is held still.
    let away = members[2];
    let present: Vec<u64> = members.iter().copied().filter(|m| *m != away).collect();
    lead(&mut cell, &mut load, &present);
    hold::still(cell.running[&away].child.id());
    let fourth = joins(&mut cell, 4);
    let mut observers = present.clone();
    observers.push(fourth);
    cell.configured(&observers, |voters, _| {
        observers.iter().all(|m| votes(voters, *m))
    });
    hold::go_on(cell.running[&away].child.id());
    members.push(fourth);
    let all = members.clone();
    cell.configured(&members, |voters, _| all.iter().all(|m| votes(voters, *m)));
    lead(&mut cell, &mut load, &members);
    acknowledged(&mut cell, &mut load, 4);
    every_object_reads_back(
        &mut cell,
        &mut load,
        &members,
        "four nodes, one joined while another was away",
    );

    // The fifth joins; then the fourth is killed and started again.
    let fifth = joins(&mut cell, 5);
    members.push(fifth);
    let all = members.clone();
    cell.configured(&members, |voters, _| all.iter().all(|m| votes(voters, *m)));
    lead(&mut cell, &mut load, &members);
    acknowledged(&mut cell, &mut load, 4);
    every_object_reads_back(&mut cell, &mut load, &members, "five nodes");
    let alive: Vec<u64> = members.iter().copied().filter(|m| *m != fourth).collect();
    lead(&mut cell, &mut load, &alive);
    cell.kill(fourth);
    cell.start(fourth);
    cell.introduce();
    cell.enlisted(fourth);
    cell.configured(&members, |voters, _| all.iter().all(|m| votes(voters, *m)));
    lead(&mut cell, &mut load, &members);
    acknowledged(&mut cell, &mut load, 4);
    every_object_reads_back(
        &mut cell,
        &mut load,
        &members,
        "five nodes, one killed and started again",
    );

    // Back to two voters: the fifth, fourth and third leave in order, highest first, each its
    // votes out of every range (a learner of the root range alone) and its volume out of
    // placement, serving its chunks until repair moves them.
    let staying: Vec<u64> = members[..2].to_vec();
    lead(&mut cell, &mut load, &staying);
    for leaving in members[2..].iter().rev() {
        cell.say_to(*leaving, b"leave\n");
        cell.until(|c| holds(c.leaving.contains(leaving)));
    }
    let gone: Vec<u64> = members[2..].to_vec();
    cell.configured(&staying, |voters, _| {
        staying.iter().all(|m| votes(voters, *m)) && gone.iter().all(|m| !votes(voters, *m))
    });
    acknowledged(&mut cell, &mut load, 4);
    every_object_reads_back(&mut cell, &mut load, &members, "two voters, three leaving");
    load.end(&mut cell);
    every_object_reads_back(&mut cell, &mut load, &staying, "the end");
}

/// `mantle init`'s arguments for a cell of three under `root`, stating bounds for the five nodes it
/// may hold: each founder with a volume of its own in a domain of its own.
fn three_of_five(root: &Path) -> Vec<String> {
    let mut args = Vec::new();
    for n in 1..=3 {
        let dir = root.join(format!("v{n}"));
        std::fs::create_dir(&dir).unwrap();
        args.push("--node".into());
        args.push(format!(
            "quic=127.0.0.1:0,plane=127.0.0.1:0,domain=d{n},volume={}:256M",
            dir.display()
        ));
    }
    args.extend([
        "--max-nodes".into(),
        cell::MOST_NODES.to_string(),
        "--max-volumes".into(),
        cell::MOST_NODES.to_string(),
    ]);
    args
}

/// Whether every one of `observers` says the root range holds `member` as `state`.
fn stands(cell: &Cell, observers: &[u64], member: u64, state: &str) -> bool {
    observers.iter().all(|o| {
        cell.standings
            .get(&(*o, member))
            .is_some_and(|(said, _)| said == state)
    })
}

/// A node with nothing to move, no volume, joins a cell of three and leaves it in order: it is a
/// voter of every range, then of none, removed, says it left and exits 0, and the cell forgets it.
/// Another, joined and then stated gone by an operator on a founder's standard input, is removed
/// and forgotten, and its identity is refused at every founder from then. Every object reads back
/// byte for byte through both doors before and after.
#[test]
fn a_member_with_nothing_to_move_leaves_whole_and_one_stated_gone_is_refused() {
    let mut cell = Cell::made(three_of_five);
    let founders: Vec<u64> = cell.dirs.keys().copied().collect();
    for founder in &founders {
        cell.enlisted(*founder);
    }
    let made = cell.objects(vec![(
        founders[0],
        Op::Create {
            bucket: BUCKET.into(),
            id: 1,
        },
        Vec::new(),
    )]);
    done(&made[0], "the bucket");
    let access = s3::Key::of(&cell.out());
    let region = cell.s3[&founders[0]].1.clone();
    let (certificate, key) = cell.pki.objects[&object_client(CLIENT + 1)].clone();
    let mut load = Steady::start(Identity {
        id: object_client(CLIENT + 1),
        certificate,
        key,
        authority: cell.pki.authority_der.clone(),
        access,
        region,
    });
    lead(&mut cell, &mut load, &founders);
    acknowledged(&mut cell, &mut load, 6);
    every_object_reads_back(&mut cell, &mut load, &founders, "a cell of three");

    // A node with no volume joins, a voter of every range once caught up.
    let light = cell.grant("quic=127.0.0.1:0,plane=127.0.0.1:0,domain=light");
    cell.start(light);
    cell.introduce();
    cell.enlisted(light);
    let mut all = founders.clone();
    all.push(light);
    cell.configured(&all, |voters, _| all.iter().all(|m| votes(voters, *m)));
    acknowledged(&mut cell, &mut load, 6);
    // It leaves whole: its votes out, removed, and its process exits well.
    let exited = cell.leave(light);
    assert!(exited.success(), "the node that left exited {exited}");
    assert!(cell.leaving.contains(&light) && cell.left.contains(&light));
    cell.configured(&founders, |voters, learners| {
        !votes(voters, light) && !learners.contains(&light)
    });
    cell.until(|c| holds(stands(c, &founders, light, "delisted")));
    acknowledged(&mut cell, &mut load, 6);
    every_object_reads_back(&mut cell, &mut load, &founders, "after a member left whole");

    // Another joins, and an operator states it gone.
    let doomed = cell.grant("quic=127.0.0.1:0,plane=127.0.0.1:0,domain=doomed");
    cell.start(doomed);
    cell.introduce();
    cell.enlisted(doomed);
    let mut all = founders.clone();
    all.push(doomed);
    cell.configured(&all, |voters, _| all.iter().all(|m| votes(voters, *m)));
    cell.say_to(founders[0], format!("gone {doomed}\n").as_bytes());
    cell.until(|c| c.gone.get(&doomed).map(|said| said.contains("Gone")));
    cell.configured(&founders, |voters, learners| {
        !votes(voters, doomed) && !learners.contains(&doomed)
    });
    cell.until(|c| holds(stands(c, &founders, doomed, "delisted")));
    // Its identity refused at every founder: each link it keeps ends refused.
    cell.until_by(Progress::Links("Identity"), |c| {
        holds(founders.iter().all(|f| {
            c.links
                .get(&(doomed, *f))
                .is_some_and(|link| link.ends_with("Identity"))
        }))
    });
    acknowledged(&mut cell, &mut load, 6);
    load.end(&mut cell);
    every_object_reads_back(
        &mut cell,
        &mut load,
        &founders,
        "after a member was stated gone",
    );
}

/// A node's `longest-poll` line, as its process writes it (`process::longest_poll_line`), parses
/// to the poll it says, which a refused read's dump shows; a line of another shape parses to none.
#[test]
fn a_longest_poll_line_parses_to_its_poll() {
    assert_eq!(
        parse("longest-poll 3072000001"),
        Some(Line::LongestPoll(Duration::from_nanos(3_072_000_001)))
    );
    assert_eq!(parse("longest-poll"), None);
    assert_eq!(parse("longest-poll 3.072s"), None);
}

/// A node's `stall` line, as its process writes it (`process::stall_line`), parses to the stall it
/// says, which a refused read's dump and a quiet watch's show: its shard, its task's spawn site and
/// label, its hold and CPU time, whose the time was and the reports left unsaid; a line of another
/// shape parses to none.
#[test]
fn a_stall_line_parses_to_its_stall() {
    assert_eq!(
        parse("stall 2 crates/node/src/listener/mod.rs:396 PutObject 3072000001 12000 blocking 4"),
        Some(Line::Stall(
            2,
            Stall {
                site: "crates/node/src/listener/mod.rs:396".to_owned(),
                label: "PutObject".to_owned(),
                held: Duration::from_nanos(3_072_000_001),
                cpu: Some(Duration::from_nanos(12_000)),
                whose: "blocking".to_owned(),
                missed: 4,
            }
        ))
    );
    assert_eq!(
        parse("stall 0 - - 8000000 - unknown 0"),
        Some(Line::Stall(
            0,
            Stall {
                site: "-".to_owned(),
                label: "-".to_owned(),
                held: Duration::from_nanos(8_000_000),
                cpu: None,
                whose: "unknown".to_owned(),
                missed: 0,
            }
        ))
    );
    assert_eq!(parse("stall 0 - - 8000000 - unknown"), None);
    assert_eq!(parse("stall 0 - - 8ms - unknown 0"), None);
    assert_eq!(parse("stall x - - 8000000 - unknown 0"), None);
}
