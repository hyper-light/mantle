//! hyper-transport driven on hyper-rt (docs/runtime.md §14): two endpoints over real UDP sockets on
//! loopback, each owned by a task on one shard — the asker in the root task, the server in a task of its
//! own — the scenario hyper-tokio's end-to-end test runs: request and reply in every class, bodies of
//! megabytes both ways, every byte checked by the shared helpers. The asker races every wait against its
//! own 1 ms hyper-rt sleep, so the driver's future is dropped mid-wait over and over (cancel safety).

#![allow(
    clippy::unwrap_in_result,
    clippy::type_complexity,
    clippy::too_many_arguments,
    clippy::string_slice,
    clippy::cast_possible_truncation,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    dead_code,
    missing_docs
)]

#[path = "../../hyper-transport/tests/common/mod.rs"]
mod common;

use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::pin::pin;
use std::task::Poll;
use std::time::{Duration, Instant};

use common::*;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt_transport::{Driver, Io};
use hyper_transport::{Config, Endpoint, Event, Fixed};

/// The period exchanges are judged at.
const PERIOD: Duration = Duration::from_secs(2);
/// The asker's own cadence, which every wait races.
const TICK_NS: u64 = 1_000_000;
/// The failure guard of a wait, far past anything loopback needs.
const GUARD: Duration = Duration::from_secs(120);
/// Datagrams a system call carries: one QUIC initial window (RFC 9002 §7.2).
const IO: Io = Io { batch: 10 };

type Drv = Driver<Mantle, Fixed, Book>;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
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

fn endpoint(
    pki: &Pki,
    own: &(Vec<u8>, Vec<u8>),
    other: &[u8],
    peer: u64,
    listen: bool,
) -> Endpoint<Mantle, Fixed, Book> {
    let mut book = Book::default();
    book.add(other, peer, Role::Node);
    let config = Config {
        tls: hyper_transport::tls::Tls::mutual(&credentials(&pki.root, &own.0, &own.1), listen)
            .unwrap(),
        role: Role::Node,
        limits: limits(),
        congestion: Default::default(),
    };
    Endpoint::new(config, Fixed::new(512 << 20, 64), book, Instant::now()).unwrap()
}

/// The driver's next event, or the tick, whichever first; the loser's future is dropped.
async fn next(driver: &mut Drv) -> Option<Event<Mantle>> {
    let mut event = pin!(driver.event());
    let mut tick = pin!(hyper_rt::futures::sleep(TICK_NS));
    poll_fn(|context| {
        if let Poll::Ready(event) = event.as_mut().poll(context) {
            return Poll::Ready(Some(event.unwrap()));
        }
        if tick.as_mut().poll(context).is_ready() {
            return Poll::Ready(None);
        }
        Poll::Pending
    })
    .await
}

#[test]
fn every_class_of_exchange_crosses_on_a_hyper_rt_shard() {
    let pki = Pki::new();
    let one = pki.issue(&name(1));
    let two = pki.issue(&name(2));
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let report = rt
        .block_on(async move {
            let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let mut server_driver: Drv =
                Driver::bind(endpoint(&pki, &two, &one.0, 1, true), loopback, IO).unwrap();
            let server_addr = server_driver.local_addr().unwrap();
            hyper_rt::futures::spawn_detached(async move {
                let mut server = Server::new();
                loop {
                    let Ok(event) = server_driver.event().await else {
                        return;
                    };
                    server.on_event(server_driver.endpoint(), event);
                    while let Some(event) = server_driver.endpoint().poll_event() {
                        server.on_event(server_driver.endpoint(), event);
                    }
                    server.advance_all(server_driver.endpoint());
                }
            })
            .unwrap();

            let mut driver: Drv =
                Driver::bind(endpoint(&pki, &one, &two.0, 2, false), loopback, IO).unwrap();
            let mut asker = Asker::new();
            let began = Instant::now();
            let now = driver.clock().unwrap();
            driver.endpoint().connect(now, 2, server_addr).unwrap();
            let mut dropped = 0u64;
            let wait = async |driver: &mut Drv,
                                  asker: &mut Asker,
                                  done: &dyn Fn(&Asker) -> bool,
                                  dropped: &mut u64| {
                while !done(asker) {
                    match next(driver).await {
                        Some(event) => {
                            asker.clock = driver.clock().unwrap();
                            asker.on_event(driver.endpoint(), event);
                            while let Some(event) = driver.endpoint().poll_event() {
                                asker.on_event(driver.endpoint(), event);
                            }
                        }
                        None => *dropped += 1,
                    }
                    asker.advance_all(driver.endpoint());
                    assert!(began.elapsed() < GUARD, "never held: {:?}", asker.events.iter().rev().take(8).collect::<Vec<_>>());
                }
            };
            wait(&mut driver, &mut asker, &|asker| !asker.connected.is_empty(), &mut dropped).await;
            let handshake = began.elapsed();
            let now = driver.clock().unwrap();
            let node = driver.endpoint();
            asker
                .ask(node, now, 2, (Kind::Vote, Class::Control), 1, None, PERIOD)
                .unwrap();
            asker
                .ask(node, now, 2, (Kind::Put, Class::Request), 2, Some(0), PERIOD)
                .unwrap();
            asker
                .ask(node, now, 2, (Kind::Snapshot, Class::Bulk), 3, Some(8 << 20), PERIOD)
                .unwrap();
            for seed in 10..26 {
                asker
                    .ask(node, now, 2, (Kind::Get, Class::Request), seed, Some(64 << 10), PERIOD)
                    .unwrap();
            }
            asker.advance_all(driver.endpoint());
            let exchanges_began = Instant::now();
            wait(&mut driver, &mut asker, &|asker| asker.finished(), &mut dropped).await;
            let took = exchanges_began.elapsed();
            for asked in &asker.asked {
                assert_eq!(asked.refused, None, "{asked:?}");
                assert_eq!((asked.reply, asked.read), (Some(asked.body), asked.body), "{asked:?}");
            }
            let stats = driver.stats();
            format!(
                "handshake {handshake:?}; 19 exchanges, 9.0 MiB each way, in {took:?}; the driver's future \
                 dropped mid-wait {dropped} times; {} datagrams sent in {} calls, {} received in {} calls",
                stats.sent, stats.send_calls, stats.received, stats.receive_calls
            )
        })
        .unwrap();
    eprintln!("{report}");
}
