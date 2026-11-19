//! Standard output over a long run, and a write that fails (docs/runtime.md §6.2): a `mantle serve` that
//! says a line a write and a flush, as its harness reads them, and the line it says once no one reads.
//!
//! mantle's `bench doors` of 100,000 objects ran its three servers 50 minutes, their output a pipe the
//! harness reads; each server ended on `DriverRefused { call: "write(stdout)", code: None }`, the writer
//! thread's sticky failure with the OS's error dropped. A pipe whose reader is gone refuses a write
//! `EPIPE`, and that refusal said nothing of itself.
//!
//! The test runs itself as children on a runtime with one shard, each saying lines through a pool of one
//! buffer, as `mantle serve` does. Expect: a long run reaches a slow pipe reader and a file whole and in
//! order; a write once the reader has gone is refused with the OS's `EPIPE`, and so is every call after
//! it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc,
    clippy::cast_possible_truncation
)]

use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};

use hyper_rt::RtError;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::stdio;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 64,
        timers_per_shard: 64,
        interests_per_shard: 64,
        ring_entries: 64,
        step_budget_ns: 1_000_000,
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

/// Format: the variable that makes this test the child, its value the lines it says.
const CHILD: &str = "HYPER_RT_STDOUT_LONG_RUN_CHILD";
/// Shape: `mantle serve`'s pool, one buffer of a page (crates/node `process.rs`).
const DEPTH: usize = 1;
const CHUNK: usize = 4096;
/// Shape: the lines of the long run, a few hundred pipe buffers' worth (64 KiB on Linux).
const LINES: usize = 200_000;

/// Line `i`, shaped as a server's status line.
fn line(i: usize) -> String {
    format!("status {i:032x} leader {} commit {}\n", i % 3, i * 7)
}

/// Says `lines` lines a write and a flush each; the first refusal ends it, said on standard error with
/// the refusal of a write and a flush after it.
fn child(lines: usize) {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async move {
        let mut output = stdio::stdout(DEPTH, CHUNK).unwrap();
        for i in 0..lines {
            let said = match output.write_all(line(i).as_bytes()).await {
                Ok(()) => output.flush().await,
                Err(e) => Err(e),
            };
            if let Err(first) = said {
                let write = output.write_all(b"after\n").await.err();
                let flush = output.flush().await.err();
                eprintln!("refused at {i}: {first:?}; then {write:?}; then {flush:?}");
                std::process::exit(3);
            }
        }
    })
    .unwrap();
}

/// This test's binary as a child saying `lines` lines, its standard output `out`.
fn spawn(test: &str, lines: usize, out: Stdio) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture", "--quiet"])
        .env(CHILD, lines.to_string())
        .stdin(Stdio::null())
        .stdout(out)
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

/// Runs the child when this process is one.
fn as_child() -> bool {
    match std::env::var(CHILD) {
        Ok(lines) => {
            child(lines.parse().unwrap());
            true
        }
        Err(_) => false,
    }
}

/// The status lines of `text`, each checked to be line `i` in order; their count.
fn counted(text: impl BufRead) -> usize {
    let mut n = 0;
    for read in text.lines() {
        let read = read.unwrap();
        if read.starts_with("status ") {
            assert_eq!(format!("{read}\n"), line(n), "line {n} whole and in order");
            n += 1;
        }
    }
    n
}

#[test]
fn a_long_run_of_lines_reaches_a_slow_pipe_reader_whole() {
    if as_child() {
        return;
    }
    let mut child = spawn(
        "a_long_run_of_lines_reaches_a_slow_pipe_reader_whole",
        LINES,
        Stdio::piped(),
    );
    // A reader a few bytes a read, far slower than the writer: the pipe fills, and the writer thread
    // waits in its write again and again.
    let n = counted(BufReader::with_capacity(16, child.stdout.take().unwrap()));
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "the child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(n, LINES, "every line reached the reader");
}

#[test]
fn a_long_run_of_lines_reaches_a_file_whole() {
    if as_child() {
        return;
    }
    let path =
        std::env::temp_dir().join(format!("hyper-rt-stdout-long-run-{}", std::process::id()));
    let file = std::fs::File::create(&path).unwrap();
    let child = spawn(
        "a_long_run_of_lines_reaches_a_file_whole",
        LINES,
        file.into(),
    );
    let output = child.wait_with_output().unwrap();
    let mut text = String::new();
    std::fs::File::open(&path)
        .unwrap()
        .read_to_string(&mut text)
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(
        output.status.success(),
        "the child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        counted(text.as_bytes()),
        LINES,
        "every line reached the file"
    );
}

#[test]
fn a_write_once_the_reader_is_gone_is_refused_with_the_os_code() {
    if as_child() {
        return;
    }
    let mut child = spawn(
        "a_write_once_the_reader_is_gone_is_refused_with_the_os_code",
        LINES,
        Stdio::piped(),
    );
    // The reader reads a thousand lines, then goes, as a harness that ended does.
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut read = String::new();
    for _ in 0..1000 {
        read.clear();
        reader.read_line(&mut read).unwrap();
    }
    drop(reader);
    let output = child.wait_with_output().unwrap();
    let said = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(3),
        "the child was refused: {said}"
    );
    #[cfg(unix)]
    {
        // EPIPE, 32 on Linux and macOS (errno(3)), for the refused write and each call after it.
        let refused = format!(
            "{:?}",
            RtError::DriverRefused {
                call: "write(stdout)",
                code: Some(32),
            }
        );
        assert_eq!(
            said.matches(&refused).count(),
            3,
            "every refusal says EPIPE: {said}"
        );
    }
    #[cfg(not(unix))]
    assert!(
        !said.contains("code: None"),
        "every refusal carries the OS's code: {said}"
    );
}
