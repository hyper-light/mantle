//! DIAG (ci-health, never merged): the wake probe beside the host's load, at the budget mantle's test
//! cells state (40 ms) and at the node's derived one, and the lateness of a 1 ms timed wait.
#![allow(clippy::unwrap_used, clippy::disallowed_macros, clippy::disallowed_methods, clippy::arithmetic_side_effects)]

use std::time::{Duration, Instant};

fn probe(budget: Duration, rounds: usize) {
    let mut refused = 0;
    for round in 0..rounds {
        let began = Instant::now();
        let result = hyper_rt::machine::calibration::Calibration::measure(budget, 0);
        let took = began.elapsed();
        match result {
            Ok(c) => eprintln!(
                "DIAGWAKE probe {budget:?} #{round}: ok samples={} rounds={} quick={} placement={:?} mean={}ns took={took:?}",
                c.wake.samples, c.wake.rounds, c.wake.quick, c.wake.placement, c.wake.mean_ns
            ),
            Err(e) => {
                refused += 1;
                eprintln!("DIAGWAKE probe {budget:?} #{round}: REFUSED {e:?} took={took:?}");
            }
        }
    }
    eprintln!("DIAGWAKE SUMMARY budget={budget:?} refused={refused} of {rounds}");
}

#[test]
fn diag_wake_probe_at_the_cells_budget() {
    let rounds: usize = std::env::var("DIAG_ROUNDS").ok().and_then(|r| r.parse().ok()).unwrap_or(30);
    probe(Duration::from_millis(40), rounds);
}

#[test]
fn diag_wake_probe_at_the_derived_budget() {
    let rounds: usize = std::env::var("DIAG_ROUNDS").ok().and_then(|r| r.parse().ok()).unwrap_or(30);
    probe(mantle_node::derive::probe_budget().unwrap(), rounds.min(10));
}

#[test]
fn diag_timed_wait_lateness() {
    let (_tx, rx) = std::sync::mpsc::channel::<()>();
    let mut late: Vec<u128> = Vec::new();
    for _ in 0..2000 {
        let now = Instant::now();
        let woke_by = now + Duration::from_millis(1);
        let _ = rx.recv_timeout(Duration::from_millis(1));
        late.push(Instant::now().saturating_duration_since(woke_by).as_micros());
    }
    late.sort_unstable();
    let at = |q: f64| late[((late.len() as f64 * q) as usize).min(late.len() - 1)];
    let total: u128 = late.iter().sum();
    eprintln!(
        "DIAGLATE 1ms recv_timeout lateness us: p50={} p90={} p99={} max={} mean={}",
        at(0.5), at(0.9), at(0.99), late[late.len() - 1], total / late.len() as u128
    );
}
