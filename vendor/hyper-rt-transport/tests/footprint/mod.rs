//! What the footprint measurements share (`connection_footprint`, under the platform's allocator,
//! and `connection_heap`, under mimalloc): the server's and the clients' endpoints, the clients'
//! process, the stepped run a learning measurement samples, and the least-squares fit its estimates
//! are made by. Each measurement is a test binary of its own, its global allocator its own.

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
    dead_code,
    missing_docs,
    unreachable_pub
)]

use std::io::Read as _;
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::common::*;
use hyper_measure::usage;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt_transport::{Driver, Io};
use hyper_transport::{AdmissionLimits, Config, Endpoint, Fixed, Limits};

pub type Drv = Driver<Mantle, Fixed, Book>;

/// Datagrams a system call carries: one QUIC initial window (RFC 9002 §7.2).
pub const IO: Io = Io { batch: 10 };
/// The server's peer id; the clients are 1 to `n`.
pub const SERVER: u64 = 0;
/// The failure guard of a wait.
pub const GUARD: Duration = Duration::from_secs(300);
/// How long the server must receive nothing for its connections to count as settled.
pub const QUIET: Duration = Duration::from_millis(500);
/// Set in the clients' process once its parent closed its standard input: the clients' tasks end.
pub static STOP: AtomicBool = AtomicBool::new(false);
/// The clients that saw their connection made, in the clients' process.
pub static CONNECTED: AtomicUsize = AtomicUsize::new(0);
/// The clients the clients' process is to have connected: all of them, or, stepped, the count its
/// standard input last named.
pub static TARGET: AtomicUsize = AtomicUsize::new(0);

/// A shard with room for `n` clients' tasks, sockets and timers.
pub fn runtime(n: usize) -> RuntimeConfig {
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
pub fn admitting(n: usize) -> Limits {
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

pub fn endpoint(
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
pub async fn turn(driver: &mut Drv) -> Option<hyper_transport::Event<Mantle>> {
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

/// Turns the driver until it holds `target` connections and no other, each admitted and with its
/// socket where the platform gives one, none closing and no handshake pending, and has received
/// nothing for [`QUIET`]; `turned` is told of each turn. A connection closed (a handshake a client's
/// retry superseded: a copy of its first ClientHello that came after the Retry) drains for three
/// probe timeouts (RFC 9000 §10.2) holding its state, and windows-2025 held one such 1.6 s past
/// the quiet: a measurement then took 17 connections' state for 16.
pub async fn settle(driver: &mut Drv, target: usize, turned: &mut impl FnMut(&mut Drv)) {
    let began = Instant::now();
    let mut last_received = 0;
    let mut quiet_since = Instant::now();
    loop {
        turn(driver).await;
        while driver.endpoint().poll_event().is_some() {}
        turned(driver);
        let stats = driver.endpoint().stats();
        let held = stats.connections;
        let admitted = stats.admission.connections;
        let received = driver.stats().received + driver.peer_stats().received;
        if received != last_received {
            last_received = received;
            quiet_since = Instant::now();
        }
        let sockets_ready = driver.peer_sockets() >= target || !cfg!(unix);
        if held == target
            && admitted == target
            && stats.admission.pending == 0
            && sockets_ready
            && quiet_since.elapsed() >= QUIET
        {
            return;
        }
        assert!(
            began.elapsed() < GUARD,
            "{held} connections held, {admitted} of {target} admitted; endpoint {stats:?}",
        );
    }
}

/// The footprint the OS charges this process now, bytes.
pub fn footprint() -> i64 {
    usage::this()
        .unwrap()
        .footprint
        .map_or(0, |bytes| bytes as i64)
}

/// The highest footprint the OS has charged this process, bytes.
pub fn peak_footprint() -> i64 {
    usage::this()
        .unwrap()
        .peak_footprint
        .map_or(0, |bytes| bytes as i64)
}

/// The socket memory the kernel charges this process's control group, bytes, where it is read
/// (Linux, cgroup v2's `memory.stat` `sock`): a connection's socket buffers are charged to a
/// container's limit though no process's footprint holds them.
pub fn cgroup_sock() -> Option<i64> {
    let relative = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = relative.lines().find_map(|line| line.strip_prefix("0::"))?;
    let stat = std::fs::read_to_string(format!("/sys/fs/cgroup{path}/memory.stat")).ok()?;
    stat.lines()
        .find_map(|line| line.strip_prefix("sock "))
        .and_then(|value| value.trim().parse().ok())
}

/// The clients' credentials, written for the clients' process: the root, the server's certificate,
/// and each client's certificate and key.
pub fn write_credentials(dir: &Path, root: &[u8], server: &[u8], clients: &[(Vec<u8>, Vec<u8>)]) {
    std::fs::write(dir.join("root.der"), root).unwrap();
    std::fs::write(dir.join("server.der"), server).unwrap();
    for (index, (certificate, key)) in clients.iter().enumerate() {
        std::fs::write(dir.join(format!("{index}.der")), certificate).unwrap();
        std::fs::write(dir.join(format!("{index}.key")), key).unwrap();
    }
}

/// The clients' process (each binary's `footprint_clients`, run by its server): `FOOTPRINT_N`
/// clients connect to `FOOTPRINT_SERVER` — all at once, or, with `FOOTPRINT_STEPPED`, as many as
/// each line of standard input names — and stay until standard input closes.
pub fn run_clients() {
    let Ok(dir) = std::env::var("FOOTPRINT_DIR") else {
        return;
    };
    let dir = Path::new(&dir).to_path_buf();
    let n: usize = std::env::var("FOOTPRINT_N").unwrap().parse().unwrap();
    let server_addr: SocketAddr = std::env::var("FOOTPRINT_SERVER").unwrap().parse().unwrap();
    let root = std::fs::read(dir.join("root.der")).unwrap();
    let server_cert = std::fs::read(dir.join("server.der")).unwrap();
    let clients: Vec<(Vec<u8>, Vec<u8>)> = (0..n)
        .map(|index| {
            (
                std::fs::read(dir.join(format!("{index}.der"))).unwrap(),
                std::fs::read(dir.join(format!("{index}.key"))).unwrap(),
            )
        })
        .collect();
    let stepped = std::env::var("FOOTPRINT_STEPPED").is_ok();
    TARGET.store(if stepped { 0 } else { n }, Ordering::Relaxed);
    std::thread::spawn(move || {
        if stepped {
            for line in std::io::stdin().lines() {
                let Ok(line) = line else { break };
                if let Ok(target) = line.trim().parse() {
                    TARGET.store(target, Ordering::Relaxed);
                }
            }
        } else {
            let mut sink = Vec::new();
            let _ = std::io::stdin().read_to_end(&mut sink);
        }
        STOP.store(true, Ordering::Relaxed);
    });
    let mut rt = LocalRuntime::new(&runtime(n)).unwrap();
    rt.block_on(async move {
        let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let to_server = [(server_cert.as_slice(), SERVER)];
        let mut made = 0;
        while !STOP.load(Ordering::Relaxed) {
            let target = TARGET.load(Ordering::Relaxed).min(clients.len());
            if made >= target {
                hyper_rt::futures::sleep(5_000_000).await.unwrap();
                continue;
            }
            let client = &clients[made];
            made += 1;
            let mut driver: Drv = Driver::bind(
                endpoint(&root, client, &to_server, false, admitting(1)),
                loopback,
                IO,
            )
            .unwrap();
            let now = driver.clock().unwrap();
            driver.endpoint().connect(now, SERVER, server_addr).unwrap();
            hyper_rt::futures::spawn_detached(async move {
                while !STOP.load(Ordering::Relaxed) {
                    if let Some(event) = turn(&mut driver).await
                        && matches!(event, hyper_transport::Event::Connected { .. })
                    {
                        CONNECTED.fetch_add(1, Ordering::Relaxed);
                    }
                    while let Some(event) = driver.endpoint().poll_event() {
                        if matches!(event, hyper_transport::Event::Connected { .. }) {
                            CONNECTED.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
            .unwrap();
        }
    })
    .unwrap();
}

/// The steps a stepped run takes: the connections held at each.
pub const STEPS: [usize; 8] = [32, 64, 128, 256, 384, 512, 768, 1024];
/// The most bytes a step's churn holds apart from the connections, a stand-in for what else a
/// node holds (its sessions' rows, messages, caches), charged to its budget as the connections are.
pub const NOISE_MAX: usize = 24 << 20;

/// A deterministic generator for the churn's sizes.
pub fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// What a stepped run's server reads beside what every run reads, and where its work for its
/// connections begins and ends: each binary's own.
pub trait Probe {
    /// Before the server's endpoint is made.
    fn begin(&self) {}
    /// The server's work for its connections begins or resumes.
    fn enter(&self) {}
    /// The server's work for its connections pauses: the churn is made, the step sampled.
    fn exit(&self) {}
    /// The server's driver turned (and once as it is made), within its work.
    fn turned(&self, _driver: &mut Drv) {}
    /// The binary's own measures at a settled step, read between [`Probe::exit`] and the next
    /// [`Probe::enter`].
    fn measures(&self, driver: &mut Drv) -> Vec<f64>;
    /// After the run.
    fn end(&self) {}
}

/// One sample of a stepped run: the connections held, the OS's footprint and its highest, the
/// kernel's socket memory where it is charged (-1 where not), the bytes the run held apart from its
/// connections (its churn), and the binary's own measures.
#[derive(Clone, Debug)]
pub struct Sample {
    pub n: f64,
    pub footprint: f64,
    pub peak_footprint: f64,
    pub sock: f64,
    pub noise: f64,
    pub measures: Vec<f64>,
}

/// A stepped run's server, in a process of its own: its clients' process told each step's count on
/// its standard input, each step settled and sampled, the churn remade between steps where `noisy`.
pub fn stepped_run(noisy: bool, probe: impl Probe + 'static) -> Vec<Sample> {
    let most = *STEPS.last().unwrap();
    let pki = Pki::new();
    let server = pki.issue(&name(SERVER));
    let clients: Vec<(Vec<u8>, Vec<u8>)> = (1..=most as u64)
        .map(|peer| pki.issue(&name(peer)))
        .collect();
    let dir = std::env::temp_dir().join(format!("learning-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    write_credentials(&dir, &pki.root, &server.0, &clients);
    let peers: Vec<(Vec<u8>, u64)> = clients
        .iter()
        .enumerate()
        .map(|(index, client)| (client.0.clone(), index as u64 + 1))
        .collect();
    let root = pki.root.clone();
    let mut rt = LocalRuntime::new(&runtime(most)).unwrap();
    rt.block_on(async move {
        use std::io::Write as _;
        let peers: Vec<(&[u8], u64)> = peers
            .iter()
            .map(|(certificate, peer)| (certificate.as_slice(), *peer))
            .collect();
        let loopback: SocketAddr = "127.0.0.1:0".parse().unwrap();
        probe.begin();
        probe.enter();
        let mut driver: Drv = Driver::bind(
            endpoint(&root, &server, &peers, true, admitting(most)),
            loopback,
            IO,
        )
        .unwrap();
        probe.turned(&mut driver);
        probe.exit();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["footprint_clients", "--exact", "--ignored", "--nocapture"])
            .env("FOOTPRINT_DIR", &dir)
            .env("FOOTPRINT_N", most.to_string())
            .env("FOOTPRINT_STEPPED", "1")
            .env("FOOTPRINT_SERVER", driver.local_addr().unwrap().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        let mut churn: Vec<Vec<u8>> = Vec::new();
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut samples = Vec::new();
        for target in STEPS {
            writeln!(stdin, "{target}").unwrap();
            stdin.flush().unwrap();
            probe.enter();
            settle(&mut driver, target, &mut |driver| probe.turned(driver)).await;
            probe.exit();
            // The churn remade: what it held let go, then blocks of random sizes to a random
            // total, each written so its pages are resident, as a node's rows are.
            churn.clear();
            churn.shrink_to_fit();
            let mut noise = 0usize;
            if noisy {
                let total = (next(&mut state) as usize) % NOISE_MAX;
                while noise < total {
                    let size = 1024 + (next(&mut state) as usize) % (1 << 20);
                    let mut block = vec![0u8; size];
                    block.iter_mut().step_by(4096).for_each(|byte| *byte = 1);
                    noise += size;
                    churn.push(block);
                }
            }
            samples.push(Sample {
                n: driver.endpoint().stats().connections as f64,
                footprint: footprint() as f64,
                peak_footprint: peak_footprint() as f64,
                sock: cgroup_sock().map_or(-1.0, |sock| sock as f64),
                noise: noise as f64,
                measures: probe.measures(&mut driver),
            });
        }
        probe.end();
        drop(stdin);
        child.wait().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        samples
    })
    .unwrap()
}

/// A stepped run's samples, printed for the measurement that runs the server's process.
pub fn print_samples(samples: &[Sample]) {
    for sample in samples {
        let measures: Vec<String> = sample.measures.iter().map(f64::to_string).collect();
        println!(
            "SAMPLE {} {} {} {} {} {}",
            sample.n,
            sample.footprint,
            sample.peak_footprint,
            sample.sock,
            sample.noise,
            measures.join(" ")
        );
    }
}

/// The samples of a stepped run whose server is this binary's test `server`, in a process of its
/// own, `LEARNING_NOISY` telling it whether to churn.
pub fn samples_of(server: &str, noisy: bool) -> Vec<Sample> {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([server, "--exact", "--ignored", "--nocapture"])
        .env("LEARNING_NOISY", if noisy { "1" } else { "0" })
        .stderr(Stdio::inherit())
        .output()
        .unwrap();
    assert!(output.status.success(), "the stepped run's server failed");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.strip_prefix("SAMPLE "))
        .map(|line| {
            let v: Vec<f64> = line
                .split(' ')
                .filter(|field| !field.is_empty())
                .map(|field| field.parse().unwrap())
                .collect();
            Sample {
                n: v[0],
                footprint: v[1],
                peak_footprint: v[2],
                sock: v[3],
                noise: v[4],
                measures: v[5..].to_vec(),
            }
        })
        .collect()
}

/// The least squares fit of `y` on `xs` (each row its regressors) with an intercept: the
/// coefficients, the intercept first, by the normal equations.
pub fn least_squares(xs: &[Vec<f64>], y: &[f64]) -> Vec<f64> {
    let k = xs[0].len() + 1;
    let mut a = vec![vec![0.0f64; k + 1]; k];
    for (row, &target) in xs.iter().zip(y) {
        let mut full = vec![1.0];
        full.extend_from_slice(row);
        for i in 0..k {
            for j in 0..k {
                a[i][j] += full[i] * full[j];
            }
            a[i][k] += full[i] * target;
        }
    }
    // Gauss-Jordan with partial pivoting.
    for col in 0..k {
        let pivot = (col..k)
            .max_by(|&p, &q| a[p][col].abs().total_cmp(&a[q][col].abs()))
            .unwrap();
        a.swap(col, pivot);
        let div = a[col][col];
        if div.abs() < 1e-12 {
            return vec![f64::NAN; k];
        }
        for value in &mut a[col][col..] {
            *value /= div;
        }
        let pivot_row = a[col].clone();
        for (index, row) in a.iter_mut().enumerate() {
            if index != col {
                let factor = row[col];
                for (value, pivot) in row[col..].iter_mut().zip(&pivot_row[col..]) {
                    *value -= factor * pivot;
                }
            }
        }
    }
    (0..k).map(|i| a[i][k]).collect()
}

/// The slope of `y` on the connections held, from `samples`.
pub fn slope(samples: &[Sample], y: impl Fn(&Sample) -> f64) -> f64 {
    let ns: Vec<Vec<f64>> = samples.iter().map(|s| vec![s.n]).collect();
    least_squares(&ns, &samples.iter().map(y).collect::<Vec<_>>())[1]
}

/// The slope of `y` on the connections held, the churn a second regressor.
pub fn slope_beside_churn(samples: &[Sample], y: impl Fn(&Sample) -> f64) -> f64 {
    let xs: Vec<Vec<f64>> = samples.iter().map(|s| vec![s.n, s.noise]).collect();
    least_squares(&xs, &samples.iter().map(y).collect::<Vec<_>>())[1]
}

/// An estimate against the truth: its bytes, and its error in per cent.
pub fn err(estimate: f64, truth: f64) -> String {
    format!(
        "{:.0} B ({:+.0}%)",
        estimate,
        (estimate / truth - 1.0) * 100.0
    )
}
