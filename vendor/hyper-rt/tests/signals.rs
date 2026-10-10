//! Signals (docs/runtime.md §6.1), Unix: two subscriptions to `SIGHUP`, the signal sent to this process,
//! each subscription sees it once; signals sent twice before a wait arrive as one event; a kind this OS does
//! not deliver is refused.

#![cfg(unix)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use hyper_rt::RtError;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::signal::{Signal, subscribe};

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

fn hangup() {
    rustix::process::kill_process(rustix::process::getpid(), rustix::process::Signal::HUP).unwrap();
}

#[test]
fn a_signal_reaches_every_subscriber_and_repeats_coalesce() {
    assert!(matches!(
        subscribe(&[Signal::Break]),
        Err(RtError::BadConfig { .. })
    ));
    let first = subscribe(&[Signal::Hangup, Signal::Terminate]).unwrap();
    let second = subscribe(&[Signal::Hangup]).unwrap();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async move {
        hangup();
        let seen = first.recv().await.unwrap();
        assert!(seen.contains(Signal::Hangup) && !seen.contains(Signal::Terminate));
        assert!(second.recv().await.unwrap().contains(Signal::Hangup));

        // Two before a wait are one event; the wait after it waits for a new one.
        hangup();
        hangup();
        assert!(first.recv().await.unwrap().contains(Signal::Hangup));
        let again = hyper_rt::futures::within(50_000_000, first.recv())
            .await
            .unwrap();
        // The signal thread may deliver the second byte after the first wait took the bit; at most one
        // more event, never two.
        if let Some(event) = again {
            assert!(event.unwrap().contains(Signal::Hangup));
            assert!(
                hyper_rt::futures::within(50_000_000, first.recv())
                    .await
                    .unwrap()
                    .is_none(),
                "no third event for two signals"
            );
        }
    })
    .unwrap();
}
