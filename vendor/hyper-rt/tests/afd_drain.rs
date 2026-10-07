//! Windows, mantle's final review of hyper-rt, finding 4 and the second pass's finding 3: a dropped driver
//! reclaims every AFD poll block it still owes. A cancel is not synchronous, so the drop waits for the
//! completions — up to the I/O manager's own timeout for a cancelled IRP, ending as soon as the last one
//! arrives — and counts a block left to the kernel. This fixture owns its process, so the count is this
//! test's runtimes' alone, and the expectation is exact: none left.

#![cfg(windows)]
// Test harness code: a panic here is a failed test (CLAUDE.md §1).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use hyper_rt::combine::{Either, race2};
use hyper_rt::futures::{sleep, yield_now};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::udp::{Ipv4Addr, SocketAddr, UdpSocket};

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 16,
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

/// Shape: a sleep that loses to nothing here: no datagram is ever sent.
const TICK_NS: u64 = 1_000_000;

fn loopback() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

/// Do: in each of several runtimes, lose a race of a read wait (its poll cancelled when its last waiter
/// leaves) and leave a task waiting on a second socket when the runtime ends (its poll still in flight at
/// the driver's drop). Expect: every block owed was reclaimed by the drop's drain; none was left to the
/// kernel.
#[test]
fn a_dropped_driver_leaves_no_afd_poll_to_the_kernel() {
    for _ in 0..8 {
        let mut rt = LocalRuntime::new(&config()).unwrap();
        rt.block_on(async {
            let idle = UdpSocket::bind(loopback()).unwrap();
            let raced = race2(idle.readable(), sleep(TICK_NS)).await;
            assert!(matches!(raced, Either::Second(Ok(()))));
            let waiting = UdpSocket::bind(loopback()).unwrap();
            hyper_rt::futures::spawn_detached(async move {
                let _ = waiting.readable().await;
            })
            .unwrap();
            yield_now().await;
        })
        .unwrap();
        drop(rt);
    }
    assert_eq!(hyper_rt::iocp::afd_blocks_left(), 0);
}
