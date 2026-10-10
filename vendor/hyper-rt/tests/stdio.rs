//! Standard input and output (docs/runtime.md §6.2): this test runs itself as a child process with its
//! standard input a pipe; the child, on a shard, reads every byte through `stdio::stdin` and writes it back
//! through `stdio::stdout`, a mebibyte through four 4 KiB buffers each way (the buffers recycle 256 times).
//! Expect: the parent reads back exactly what it sent, between markers the child writes around it.

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

use std::io::Write;
use std::process::{Command, Stdio};

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::stdio;

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
    }
}

/// Format: the variable that makes this test the child.
const CHILD: &str = "HYPER_RT_STDIO_CHILD";
const BEGIN: &[u8] = b"<<begin>>";
const END: &[u8] = b"<<end>>";
/// Shape: the bytes sent through, far past the buffers' 16 KiB each way.
const BYTES: usize = 1 << 20;

fn child() {
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let mut input = stdio::stdin(4, 4096).unwrap();
        let mut output = stdio::stdout(4, 4096).unwrap();
        output.write_all(BEGIN).await.unwrap();
        let mut buf = [0u8; 1000];
        loop {
            let n = input.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            output.write_all(&buf[..n]).await.unwrap();
        }
        output.write_all(END).await.unwrap();
        output.flush().await.unwrap();
    })
    .unwrap();
}

#[test]
fn bytes_cross_stdin_and_stdout_without_blocking_a_shard() {
    if std::env::var_os(CHILD).is_some() {
        child();
        return;
    }
    let data: Vec<u8> = (0..BYTES).map(|i| (i % 251) as u8).collect();
    let mut process = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "bytes_cross_stdin_and_stdout_without_blocking_a_shard",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = process.stdin.take().unwrap();
    let sent = data.clone();
    let writer = std::thread::spawn(move || {
        stdin.write_all(&sent).unwrap();
    });
    let output = process.wait_with_output().unwrap();
    writer.join().unwrap();
    assert!(output.status.success());
    let out = output.stdout;
    let begin = out
        .windows(BEGIN.len())
        .position(|w| w == BEGIN)
        .expect("the child began")
        + BEGIN.len();
    let end = out
        .windows(END.len())
        .rposition(|w| w == END)
        .expect("the child ended");
    assert_eq!(end - begin, data.len(), "every byte, no more");
    assert!(out[begin..end] == data[..], "in order");
}
