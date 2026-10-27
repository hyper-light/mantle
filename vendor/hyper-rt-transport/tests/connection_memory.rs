//! What a server holds in memory for each connection it holds, measured (ignored: a measurement, run
//! in release with `--ignored --nocapture`): the server's driver runs alone on a thread whose
//! allocations are counted (`hyper_measure::alloc`, per thread), its clients on another, so what
//! is counted is the server's alone. For each row the server is made with admission for `n`
//! connections (what its tables are sized to before any connects) and then `n` clients connect and
//! go quiet, each on a socket of its own where the platform shares ports; the row reports the bytes
//! the endpoint held once made and what each connection added, live once every connection settled.
//! A node's connection bound is to be derived from this and its memory, not stated (focal's listener
//! admits 128).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    dead_code,
    missing_docs
)]

#[path = "../../hyper-transport/tests/common/mod.rs"]
mod common;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU16, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use common::*;
use hyper_measure::alloc;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt_transport::{Driver, Io};
use hyper_transport::{AdmissionLimits, Config, Endpoint, Fixed, Limits};

#[global_allocator]
static ALLOCATOR: alloc::Counting = alloc::Counting;

type Drv = Driver<Mantle, Fixed, Book>;

/// Datagrams a system call carries: one QUIC initial window (RFC 9002 §7.2).
const IO: Io = Io { batch: 10 };
/// The server's peer id; the clients are 1 to `n`.
const SERVER: u64 = 0;
/// The failure guard of a wait.
const GUARD: Duration = Duration::from_secs(120);
/// How long the server must receive nothing for its connections to count as settled.
const QUIET: Duration = Duration::from_millis(300);
/// Set when a row's server has measured: its clients' tasks end. A static, not a shared cell: one row
/// runs at a time, each from false.
static STOP: AtomicBool = AtomicBool::new(false);

/// A shard with room for `n` clients' tasks, sockets and timers.
fn runtime(n: usize) -> RuntimeConfig {
    let room = 4 * n + 64;
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: room,
        timers_per_shard: room,
        interests_per_shard: room,
        ring_entries: 256,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
        latency_objective_ns: None,
    }
}

/// Admission for `n` identities a connection each; no keep-alive and an idle bound past any row, so
/// a settled connection sends nothing.
fn admitting(n: usize) -> Limits {
    Limits {
        admission: AdmissionLimits {
            pending: n + 8,
            identities: n + 8,
            connections: n + 8,
            per_identity: 2,
        },
        keep_alive: None,
        idle_timeout: Duration::from_secs(600),
        ..limits()
    }
}

fn endpoint(
    root: &[u8],
    own: &(Vec<u8>, Vec<u8>),
    peers: &[(&[u8], u64)],
    listen: bool,
    limits: Limits,
) -> Endpoint<Mantle, Fixed, Book> {
    let mut book = Book::default();
    for (certificate, peer) in peers {
        book.add(certificate, *peer, Role::Node);
    }
    let config = Config {
        tls: hyper_transport::tls::Tls::mutual(&credentials(root, &own.0, &own.1), listen).unwrap(),
        role: Role::Node,
        limits,
        congestion: Default::default(),
    };
    Endpoint::new(config, Fixed::new(512 << 20, 64), book, Instant::now()).unwrap()
}

/// The driver's next event, or none after 5 ms, whichever first.
async fn turn(driver: &mut Drv) -> Option<hyper_transport::Event<Mantle>> {
    use std::future::Future;
    let mut event = std::pin::pin!(driver.event());
    let mut tick = std::pin::pin!(hyper_rt::futures::sleep(5_000_000));
    std::future::poll_fn(|context| {
        if let std::task::Poll::Ready(event) = event.as_mut().poll(context) {
            return std::task::Poll::Ready(event.ok());
        }
        tick.as_mut().poll(context).map(|_| None)
    })
    .await
}

/// Each client's events, for the diagnosis of one that never connected: connected, closed.
static CONNECTED_EVENTS: [AtomicU8; 512] = [const { AtomicU8::new(0) }; 512];
static CLOSED_EVENTS: [AtomicU8; 512] = [const { AtomicU8::new(0) }; 512];
/// Each client's local port.
static PORTS: [AtomicU16; 512] = [const { AtomicU16::new(0) }; 512];

/// One row: the server's live bytes once made for `n`, and once `n` clients connected and settled.
fn row(n: usize) -> (i64, i64, usize) {
    let pki = Pki::new();
    let server = pki.issue(&name(SERVER));
    let clients: Vec<(Vec<u8>, Vec<u8>)> =
        (1..=n as u64).map(|peer| pki.issue(&name(peer))).collect();
    let root = pki.root.clone();
    STOP.store(false, Ordering::Relaxed);
    let (address_to, address_from) = mpsc::channel::<SocketAddr>();
    // The clients, on a thread of their own: none of their allocations is the server thread's.
    let client_thread = {
        let (root, server_cert, clients) = (root.clone(), server.0.clone(), clients.clone());
        std::thread::spawn(move || {
            let mut rt = LocalRuntime::new(&runtime(n)).unwrap();
            rt.block_on(async move {
                let server_addr = address_from.recv().unwrap();
                let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
                let to_server = [(server_cert.as_slice(), SERVER)];
                for (index, client) in clients.iter().enumerate() {
                    let mut driver: Drv = Driver::bind(
                        endpoint(&root, client, &to_server, false, admitting(1)),
                        loopback,
                        IO,
                    )
                    .unwrap();
                    let now = driver.clock().unwrap();
                    driver.endpoint().connect(now, SERVER, server_addr).unwrap();
                    PORTS[index].store(driver.local_addr().unwrap().port(), Ordering::Relaxed);
                    CONNECTED_EVENTS[index].store(0, Ordering::Relaxed);
                    CLOSED_EVENTS[index].store(0, Ordering::Relaxed);
                    hyper_rt::futures::spawn_detached(async move {
                        let note = |event: &hyper_transport::Event<Mantle>| match event {
                            hyper_transport::Event::Connected { .. } => {
                                CONNECTED_EVENTS[index].fetch_add(1, Ordering::Relaxed);
                            }
                            hyper_transport::Event::Closed { .. } => {
                                eprintln!("client {} closed: {event:?}", index + 1);
                                CLOSED_EVENTS[index].fetch_add(1, Ordering::Relaxed);
                            }
                            _ => {}
                        };
                        while !STOP.load(Ordering::Relaxed) {
                            if let Some(event) = turn(&mut driver).await {
                                note(&event);
                            }
                            while let Some(event) = driver.endpoint().poll_event() {
                                note(&event);
                            }
                        }
                        if CONNECTED_EVENTS[index].load(Ordering::Relaxed) == 0 {
                            eprintln!(
                                "client {} never connected: closed {}; driver {:?}; peers {:?} ({} sockets); endpoint {:?}; connection {:?}",
                                index + 1,
                                CLOSED_EVENTS[index].load(Ordering::Relaxed),
                                driver.stats(),
                                driver.peer_stats(),
                                driver.peer_sockets(),
                                driver.endpoint().stats(),
                                driver.endpoint().connection_stats(SERVER),
                            );
                        }
                    })
                    .unwrap();
                }
                while !STOP.load(Ordering::Relaxed) {
                    hyper_rt::futures::sleep(5_000_000).await.unwrap();
                }
            })
            .unwrap();
        })
    };
    // The server, alone on this thread, counted.
    let mut rt = LocalRuntime::new(&runtime(n)).unwrap();
    let (made, settled, sockets) = rt
        .block_on(async move {
            let peers: Vec<(&[u8], u64)> = clients
                .iter()
                .enumerate()
                .map(|(index, client)| (client.0.as_slice(), index as u64 + 1))
                .collect();
            let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
            alloc::begin();
            let mut driver: Drv = Driver::bind(
                endpoint(&root, &server, &peers, true, admitting(n)),
                loopback,
                IO,
            )
            .unwrap();
            let made = alloc::read().live;
            address_to.send(driver.local_addr().unwrap()).unwrap();
            let began = Instant::now();
            // Every connection held and given its socket, then quiet: the facts the row needs.
            let mut last_received = 0;
            let mut quiet_since = Instant::now();
            loop {
                turn(&mut driver).await;
                while driver.endpoint().poll_event().is_some() {}
                let held = driver.endpoint().stats().connections;
                let received = driver.stats().received + driver.peer_stats().received;
                if received != last_received {
                    last_received = received;
                    quiet_since = Instant::now();
                }
                let sockets_ready = driver.peer_sockets() >= n || !cfg!(unix);
                if held >= n && sockets_ready && quiet_since.elapsed() >= QUIET {
                    break;
                }
                if began.elapsed() >= GUARD {
                    let mut listed = Vec::new();
                    driver.endpoint().established(&mut listed);
                    let mut peers: Vec<u64> = listed.iter().map(|(peer, _)| *peer).collect();
                    peers.sort_unstable();
                    let mut addresses: Vec<SocketAddr> = listed.iter().map(|(_, remote)| *remote).collect();
                    addresses.sort_unstable();
                    let twice_peer = peers.windows(2).filter(|w| w[0] == w[1]).count();
                    let twice_address = addresses.windows(2).filter(|w| w[0] == w[1]).count();
                    let missing: Vec<u64> = (1..=n as u64).filter(|peer| peers.binary_search(peer).is_err()).collect();
                    let mut ports: Vec<(u16, u64)> = (0..n).map(|i| (PORTS[i].load(Ordering::Relaxed), i as u64 + 1)).collect();
                    ports.sort_unstable();
                    let shared: Vec<(u16, u64, u64)> = ports.windows(2).filter(|w| w[0].0 == w[1].0).map(|w| (w[0].0, w[0].1, w[1].1)).collect();
                    eprintln!("clients sharing a port (port, client, client): {shared:?}");
                    let stats = driver.endpoint().stats();
                    STOP.store(true, Ordering::Relaxed);
                    std::thread::sleep(Duration::from_millis(200));
                    panic!(
                        "{held} slots, {} established ({twice_peer} peers twice, {twice_address} addresses twice, missing {missing:?}), {} sockets; endpoint {stats:?}; admission {:?}; listener {:?}; peers {:?}; rooms {:?}",
                        listed.len(),
                        driver.peer_sockets(),
                        stats.admission,
                        driver.stats(),
                        driver.peer_stats(),
                        driver.rooms(),
                    );
                }
            }
            let settled = alloc::read().live;
            let sockets = driver.peer_sockets();
            alloc::end();
            (made, settled, sockets)
        })
        .unwrap();
    STOP.store(true, Ordering::Relaxed);
    client_thread.join().unwrap();
    (made, settled, sockets)
}

#[test]
#[ignore = "a measurement: cargo test --release -p hyper-rt-transport --test connection_memory -- --ignored --nocapture"]
fn what_a_server_holds_for_each_connection() {
    println!("| connections | endpoint made | settled | added a connection | peer sockets |");
    println!("|---|---|---|---|---|");
    for n in [1usize, 64, 256] {
        let (made, settled, sockets) = row(n);
        let added = settled - made;
        println!(
            "| {n} | {made} B | {settled} B | {} B | {sockets} |",
            added / n as i64
        );
    }
}

/// What an endpoint charges a connection's state before it has observed what its connections hold
/// (`hyper_transport::state_charge`, its structures' own bytes) is the least a connection holds:
/// the connections past the first 32, each settled, add at least that much of their server's heap,
/// their rows in the endpoint's tables among it. What they hold past it is learned where the
/// endpoint runs (`connection_heap`). What the first connection makes once (its configuration's
/// state) is no connection's own, so the measure is the difference of two rows.
#[test]
fn a_connection_holds_at_least_what_it_is_charged_before_anything_is_observed() {
    let (made_less, settled_less, _) = row(32);
    let (made_more, settled_more, _) = row(64);
    let each = ((settled_more - made_more) - (settled_less - made_less)) / 32;
    println!("each connection past 32 held {each} B of its server's heap");
    let least = hyper_transport::state_charge::<Mantle>(&admitting(64));
    assert!(
        u64::try_from(each).unwrap() >= least,
        "a connection held {each} B of its server, less than its structures' {least} B"
    );
}
