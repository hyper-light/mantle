//! A write to standard output dropped before it is ready takes no buffer of the writer's pool and hands the
//! writer nothing (docs/runtime.md §6.2): mantle's client races each sink write against its transport and
//! drops a write the transport overtook, then makes it again.
//!
//! The turn charge (`crate::coop`, docs/runtime.md §3.14) made the hand-off of a buffer already taken an
//! await that may answer `Pending`, and a write dropped there dropped the buffer with it. Once every buffer
//! of the pool had gone that way, the next write waited for good for a buffer no thread would hand back:
//! the shard parked in its driver and the writer thread idled in its receive. That was mantle's CLI on
//! Linux, `mantle --client DIR cp s3://BUCKET/KEY -` on CI's one-CPU leg, its runtime on a slice of a few
//! microseconds (mantle `crates/mantle/tests/native.rs`, 2026-11-18): no wake was lost, a buffer was.
//!
//! The test runs itself as a child with standard output a pipe, on a runtime shaped as the CLI's (one
//! shard, a few-microsecond step quantum, a batch past one, an idle spin, wake tracking), its thread pinned
//! to one CPU where the OS pins (Linux), the writer thread inheriting the mask. Each round makes the CLI's
//! hand-offs: a flush (a message to the writer thread over the channel, its answer back over a one-shot),
//! then a write polled once in a poll that has held its shard past its share while the shard is wanted, so
//! its first operation goes on and its second is refused, then dropped, then the same bytes written again.
//! Expect: no write made again ever waits (every `Pending` it answers is a yield to its turn), and the
//! parent reads exactly the bytes written again, in order, none of the dropped ones.

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

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::process::{Command, Stdio};
use std::task::Poll;

use hyper_rt::futures::{now_ns, yield_now};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig, WakeTracking};
use hyper_rt::{coop, registry, stdio};

/// Shape: the step quantum, a few microseconds, as the CLI's calibrated slice is (mantle
/// `crates/mantle/src/client.rs`, `runtime_of`); the seed of the wake estimate that replaces it.
const SLICE_NS: u64 = 5_000;

/// The CLI's runtime, as `RuntimeConfig::from_calibration` shapes it, with one shard and no fixed core.
fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 4,
        timers_per_shard: 4,
        interests_per_shard: 8,
        ring_entries: 64,
        step_budget_ns: SLICE_NS,
        timer_tick_ns: 100_000,
        batch: 64,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: SLICE_NS,
        wake_tracking: Some(WakeTracking {
            prior_ns: SLICE_NS,
            shift: 4,
            idle_ratio: 1,
        }),
        latency_objective_ns: None,
    }
}

/// Format: the variable that makes this test the child.
const CHILD: &str = "HYPER_RT_STDOUT_DROPPED_WRITE_CHILD";
const BEGIN: &[u8] = b"<<begin>>";
const END: &[u8] = b"<<end>>";
/// Shape: the CLI's pool, two buffers (mantle `crates/mantle/src/client.rs`, `Stdout::take`), of a page.
const DEPTH: usize = 2;
const CHUNK: usize = 4096;
/// Shape: rounds, each a flush, a dropped write and a write made again: hundreds of times the pool's
/// buffers, where a buffer lost a round empties the pool by round [`DEPTH`].
const ROUNDS: usize = 500;

/// Round `round`'s bytes, one buffer's worth at most.
fn piece(round: usize) -> Vec<u8> {
    format!("piece {round:06}\n").into_bytes()
}

/// Pins the calling thread to the CPU it runs on, where the OS pins (Linux; the threads it starts inherit
/// the mask, as on CI's one-CPU leg). Elsewhere the OS places it.
fn pin_to_one_cpu() {
    #[cfg(target_os = "linux")]
    {
        let mut one = rustix::thread::CpuSet::new();
        one.set(rustix::thread::sched_getcpu());
        if let Err(refusal) = rustix::thread::sched_setaffinity(None, &one) {
            eprintln!(
                "stdout_dropped_write: pinning refused ({refusal}); the OS places the threads"
            );
        }
    }
}

/// Polls a write of `piece` once and drops it, as the client drops a write its transport overtook: in a
/// fresh poll (no charge made in it yet) that has held the shard past its share while the shard is wanted
/// (the task itself woken, so listed), so the write's first operation is the poll's first charge, which
/// goes on, and its second is refused: it answers `Pending` part-way.
async fn overtaken(output: &mut stdio::Stdout, piece: &[u8]) {
    yield_now().await;
    let mut write = pin!(output.write_all(piece));
    poll_fn(|cx| {
        let share = registry::with_current(|shard| shard.quantum_ns()).unwrap();
        let from = now_ns();
        while now_ns().saturating_sub(from) <= share {
            std::hint::spin_loop();
        }
        cx.waker().wake_by_ref();
        assert!(
            write.as_mut().poll(cx).is_pending(),
            "the write's second operation was refused"
        );
        assert!(coop::spent(), "the write's turn was spent");
        Poll::Ready(())
    })
    .await;
}

/// Writes `piece` again, to its end. Nothing it needs is behind the writer thread: the flush before the
/// round brought every buffer handed over back to the pool, a dropped write took none, and the writer's
/// channel has room. So every `Pending` it answers is a yield to its turn ([`coop::spent`]); one that waits
/// waits for a buffer the pool lost, which no thread will hand back. The write's own outcome.
async fn made_again(
    output: &mut stdio::Stdout,
    piece: &[u8],
    round: usize,
) -> Result<(), hyper_rt::RtError> {
    let mut write = pin!(output.write_all(piece));
    poll_fn(|cx| match write.as_mut().poll(cx) {
        Poll::Ready(wrote) => Poll::Ready(wrote),
        Poll::Pending => {
            assert!(
                coop::spent(),
                "round {round}: a write made again waits for a buffer: a dropped write took one from the pool"
            );
            Poll::Pending
        }
    })
    .await
}

fn child() {
    pin_to_one_cpu();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async {
        let mut output = stdio::stdout(DEPTH, CHUNK).unwrap();
        output.write_all(BEGIN).await.unwrap();
        for round in 0..ROUNDS {
            let piece = piece(round);
            output.flush().await.unwrap();
            overtaken(&mut output, &piece).await;
            made_again(&mut output, &piece, round).await.unwrap();
        }
        output.write_all(END).await.unwrap();
        output.flush().await.unwrap();
    })
    .unwrap();
}

#[test]
fn a_dropped_stdout_write_takes_no_buffer_and_writes_nothing() {
    if std::env::var_os(CHILD).is_some() {
        child();
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "a_dropped_stdout_write_takes_no_buffer_and_writes_nothing",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "the child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
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
    let written: Vec<u8> = (0..ROUNDS).flat_map(piece).collect();
    assert!(
        out[begin..end] == written[..],
        "each round's bytes once, in order, none of a dropped write's"
    );
}
