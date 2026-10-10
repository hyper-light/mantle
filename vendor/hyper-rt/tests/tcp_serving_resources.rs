//! Public TCP setup/accept lifetime refusals reclaim capacity for real identical retries.
//! Each case is a subprocess because the public synchronization cell limit is process-wide.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::missing_panics_doc
)]

use std::io::{Read, Write};
use std::process::Command;
use std::sync::mpsc;
use std::time::Duration;

use hyper_rt::RtError;
use hyper_rt::runtime::{Runtime, RuntimeConfig};
use hyper_rt::sync::{cell, oneshot};
use hyper_rt::tcp::{ConnectionBudget, Ipv4Addr, Serving, SocketAddr, TcpStream, serve};

/// Shape: two shards make a successful first pair precede a later setup refusal.
const SHARDS: u16 = 2;
/// Intrinsic TCP serving metadata: one count and one ended cell per actual shard.
const CELLS_PER_SHARD: usize = 2;
/// Protocol: four exact request/response bytes, one connection at a time.
const REQUEST: &[u8; 4] = b"PING";
const RESPONSE: &[u8; 4] = b"PONG";
/// Existing tcp_serve test's five-second failure bound; time never satisfies success.
const FAILURE: Duration = Duration::from_secs(5);
/// Scoped subprocess marker: isolates the configured public cell limit from other tests.
const CHILD: &str = "HYPER_RT_TCP_SERVING_RESOURCE_CHILD";

struct RestoreLimit;
impl Drop for RestoreLimit {
    fn drop(&mut self) {
        cell::set_limit(cell::MAX_CELLS);
    }
}

fn config(tasks: usize) -> RuntimeConfig {
    RuntimeConfig {
        shards: SHARDS,
        tasks_per_shard: tasks,
        timers_per_shard: tasks,
        interests_per_shard: 4,
        ring_entries: 4,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: tasks,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

fn address() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

fn listen(
    rt: &Runtime,
    budget: &ConnectionBudget,
) -> (Serving, mpsc::Receiver<Result<(), RtError>>) {
    let (done, completed) = mpsc::sync_channel(1);
    let serving = serve(
        rt,
        address(),
        1,
        budget.clone(),
        move |stream: TcpStream| {
            let done = done.clone();
            async move {
                let result = async {
                    let mut request = [0; REQUEST.len()];
                    let mut at = 0;
                    while at < request.len() {
                        let n = stream.read(&mut request[at..]).await?;
                        if n == 0 {
                            return Err(RtError::BadConfig {
                                what: "TCP ownership oracle truncated request",
                            });
                        }
                        at += n;
                    }
                    if &request != REQUEST {
                        return Err(RtError::BadConfig {
                            what: "TCP ownership oracle changed request",
                        });
                    }
                    stream.write_all(RESPONSE).await
                }
                .await;
                drop(stream);
                // A counted connection has actually closed before this positive fact is published.
                let _ = done.try_send(result);
            }
        },
    )
    .expect("reclaimed TCP setup capacity must permit the identical healthy server");
    (serving, completed)
}

fn echo(
    serving: &Serving,
    completed: &mpsc::Receiver<Result<(), RtError>>,
    budget: &ConnectionBudget,
) {
    let mut client = std::net::TcpStream::connect_timeout(&serving.local_addr(), FAILURE).unwrap();
    client.set_read_timeout(Some(FAILURE)).unwrap();
    client.set_write_timeout(Some(FAILURE)).unwrap();
    client.write_all(REQUEST).unwrap();
    let mut response = [0; RESPONSE.len()];
    client.read_exact(&mut response).unwrap();
    assert_eq!(&response, RESPONSE);
    completed.recv_timeout(FAILURE).unwrap().unwrap();
    assert_eq!(
        budget.open(),
        0,
        "actual connection close returned its public budget"
    );
    assert!(serving.open().iter().all(|(_, count)| *count == 0));
    drop(client);
}

fn identical_retry(rt: Runtime, budget: ConnectionBudget) {
    let (serving, completed) = listen(&rt, &budget);
    echo(&serving, &completed, &budget);
    rt.shutdown().unwrap();
    drop(serving);
    drop(budget);
}

fn partial_setup() {
    let _restore = RestoreLimit;
    let rt = Runtime::start(&config(2)).unwrap();
    let budget = ConnectionBudget::new(1).unwrap();
    let initial = cell::live();
    // Two ordinary owned primitives occupy one shard pair's allowance. They are real
    // resource owners, not forged arena entries, and are released before the SAME-cap retry.
    let holds = [oneshot::<()>().unwrap(), oneshot::<()>().unwrap()];
    let metadata = usize::from(SHARDS).checked_mul(CELLS_PER_SHARD).unwrap();
    let limit = initial.checked_add(metadata).unwrap();
    cell::set_limit(limit);
    let refused = serve(
        &rt,
        address(),
        1,
        budget.clone(),
        |stream: TcpStream| async move {
            drop(stream);
        },
    );
    let typed = matches!(&refused, Err(RtError::Capacity { .. }));
    let after_refusal = cell::live();
    drop(refused);
    drop(holds);
    eprintln!(
        "partial setup diagnostics: initial={initial}, after_refusal={after_refusal}, after_hold_release={}, limit={limit}",
        cell::live()
    );
    // Limit is not raised. A leaked completed shard pair prevents this public retry/echo.
    identical_retry(rt, budget);
    assert!(
        typed,
        "multi-shard serving setup did not produce its typed cell-capacity refusal"
    );
}

fn cancellation() {
    let _restore = RestoreLimit;
    let rt = Runtime::start(&config(2)).unwrap();
    let budget = ConnectionBudget::new(1).unwrap();
    let initial = cell::live();
    let metadata = usize::from(SHARDS).checked_mul(CELLS_PER_SHARD).unwrap();
    let limit = initial.checked_add(metadata).unwrap();
    cell::set_limit(limit);
    let (serving, completed) = listen(&rt, &budget);
    echo(&serving, &completed, &budget);
    // Runtime shutdown actually destroys its pending accept futures and joins the owners.
    rt.shutdown().unwrap();
    drop(serving);
    eprintln!(
        "accept cancellation diagnostics: initial={initial}, after_shutdown={}, limit={limit}",
        cell::live()
    );
    identical_retry(Runtime::start(&config(2)).unwrap(), budget);
}

struct DroppedHandler {
    cloned: bool,
    done: mpsc::SyncSender<()>,
}
impl Clone for DroppedHandler {
    fn clone(&self) -> Self {
        Self {
            cloned: true,
            done: self.done.clone(),
        }
    }
}
impl Drop for DroppedHandler {
    fn drop(&mut self) {
        if self.cloned {
            let _ = self.done.try_send(());
        }
    }
}

fn refused_spawn() {
    let _restore = RestoreLimit;
    let rt = Runtime::start(&config(1)).unwrap();
    // Every actual shard's sole task slot is admitted before serving. A pending future
    // cannot complete spontaneously, so the accept spawn is refused by actual task capacity.
    for &shard in rt.shard_ids() {
        rt.spawn_on_with_receipt(shard, std::future::pending::<()>())
            .unwrap()
            .wait_blocking()
            .unwrap();
    }
    let budget = ConnectionBudget::new(1).unwrap();
    let initial = cell::live();
    let metadata = usize::from(SHARDS).checked_mul(CELLS_PER_SHARD).unwrap();
    let limit = initial.checked_add(metadata).unwrap();
    cell::set_limit(limit);
    #[cfg(target_os = "linux")]
    let acceptors = usize::from(SHARDS);
    #[cfg(not(target_os = "linux"))]
    let acceptors = 1;
    let (done, dropped) = mpsc::sync_channel(acceptors);
    let handler = DroppedHandler {
        cloned: false,
        done,
    };
    let serving = serve(
        &rt,
        address(),
        1,
        budget.clone(),
        move |stream: TcpStream| {
            let handler = handler.clone();
            async move {
                let _handler = handler;
                drop(stream);
            }
        },
    )
    .unwrap();
    for _ in 0..acceptors {
        // Actual public handler clone destruction confirms refused accept-future ownership;
        // native shutdown below then guarantees ALL its fields have finished destruction.
        dropped.recv_timeout(FAILURE).unwrap();
    }
    rt.shutdown().unwrap();
    drop(serving);
    eprintln!(
        "refused spawn diagnostics: initial={initial}, after_shutdown={}, limit={limit}",
        cell::live()
    );
    identical_retry(Runtime::start(&config(2)).unwrap(), budget);
}

fn isolated(name: &str, case: fn()) {
    if std::env::var_os(CHILD).is_some() {
        case();
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env(CHILD, name)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "isolated TCP resource oracle failed\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn partial_setup_reclaims_capacity_for_identical_echo_retry() {
    isolated(
        "partial_setup_reclaims_capacity_for_identical_echo_retry",
        partial_setup,
    );
}
#[test]
fn cancelled_acceptors_reclaim_capacity_for_identical_echo_retry() {
    isolated(
        "cancelled_acceptors_reclaim_capacity_for_identical_echo_retry",
        cancellation,
    );
}
#[test]
fn refused_accept_spawns_reclaim_capacity_for_identical_echo_retry() {
    isolated(
        "refused_accept_spawns_reclaim_capacity_for_identical_echo_retry",
        refused_spawn,
    );
}
