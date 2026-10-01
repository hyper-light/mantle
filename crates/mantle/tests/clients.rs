//! Logical clients cost no threads: `mantle bench log`, a real process on a real disk, runs as
//! many threads at ten and a hundred times its drivers' replicas as at once its drivers'
//! count, as the OS counts them (docs/design/measurement.md §10; research/26 §8, test 2).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

use std::process::Command;

/// The replicas, threads and idle threads of each `append` row of `mantle bench log`'s output.
fn rows(out: &str) -> Vec<(usize, usize, usize)> {
    out.lines()
        .filter_map(|line| {
            let tokens: Vec<&str> = line.split_whitespace().collect();
            // "append", the size and its unit, the replicas, the threads, the idle threads.
            if tokens.first() != Some(&"append") {
                return None;
            }
            Some((
                tokens[3].parse().unwrap(),
                tokens[4].parse().unwrap(),
                tokens[5].parse().unwrap(),
            ))
        })
        .collect()
}

#[test]
fn bench_log_runs_the_same_threads_at_ten_and_a_hundred_times_the_replicas() {
    let cores = std::thread::available_parallelism().unwrap().get();
    let counts = [cores, 10 * cores, 100 * cores];
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_mantle"))
        .args(["bench", "log"])
        .arg(dir.path())
        .args([
            "--skip-device",
            "--seconds",
            "0.2",
            "--sizes",
            "128",
            "--replicas",
        ])
        .arg(
            counts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
        )
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{text}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    eprintln!("{text}");
    let rows = rows(&text);
    assert_eq!(
        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        counts,
        "{text}"
    );
    let threads: Vec<usize> = rows.iter().map(|r| r.1).collect();
    assert!(threads[0] > 0, "{text}");
    assert!(
        threads.iter().all(|&t| t == threads[0]),
        "threads {threads:?} at replicas {counts:?}"
    );
    // Above what the process runs idle (the main thread, and what the operating system runs in
    // every process): a driver a core and the log's writer, nothing a replica.
    let added: Vec<usize> = rows.iter().map(|r| r.1.saturating_sub(r.2)).collect();
    assert!(
        added.iter().all(|&a| a <= cores + 1),
        "{added:?} above idle {:?} for {cores} cores",
        rows.iter().map(|r| r.2).collect::<Vec<_>>()
    );
}
