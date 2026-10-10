//! Simulation must drain ready cancellation after a virtual park, but retain an
//! idle service without blocking, fabricating completion, or advancing dead timers.
#![allow(
    clippy::cognitive_complexity,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use hyper_rt::RtError;
use hyper_rt::control::Control;
use hyper_rt::futures;
use hyper_rt::registry;
use hyper_rt::runtime::RuntimeConfig;
use hyper_rt::sim::SimRuntime;
use hyper_rt::sync;
use hyper_rt::task::{Admission, SpawnRequest};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::task::Poll;

struct Returned(SyncSender<()>);
impl Drop for Returned {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

#[test]
fn a_lost_simulated_driver_keeps_idle_service_ownership_until_external_completion() {
    // The simulator drives one service directly; no second runtime task is needed.
    // Clock values and seed reuse the existing differential lifecycle fixture.
    let cfg = RuntimeConfig {
        shards: 1,
        tasks_per_shard: 1,
        timers_per_shard: 1,
        interests_per_shard: 1,
        ring_entries: 1,
        step_budget_ns: 1_000_000_000,
        timer_tick_ns: 100_000,
        batch: 1,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    };
    let mut sim = SimRuntime::new(&cfg, 3).unwrap();
    let shard = sim.shard_ids().first().copied().unwrap();
    let (returned, heard) = sync_channel(1);
    let (first, pending) = sync_channel(1);
    let (closing_fact, closing) = sync_channel(1);
    let (release, mut retirement) = sync::channel(1).unwrap();
    let capture = Returned(returned);
    let tick = cfg.timer_tick_ns;
    let (request, receipt) = SpawnRequest::with_receipt(
        Box::pin(async move {
            let _capture = capture;
            let socket = hyper_rt::udp::UdpSocket::bind(([127, 0, 0, 1], 0)).unwrap();
            let mut cancel = pin!(futures::cancelled());
            let mut noted = false;
            poll_fn(|cx| match cancel.as_mut().poll(cx) {
                Poll::Pending => {
                    if !noted {
                        first.try_send(()).unwrap();
                        noted = true;
                    }
                    Poll::Pending
                }
                Poll::Ready(result) => Poll::Ready(result),
            })
            .await
            .unwrap();
            // This Target::Sim path used to bypass the OS registration refusal.
            assert_eq!(socket.readable().await, Err(RtError::DriverLost));
            assert_eq!(futures::sleep(tick).await, Err(RtError::DriverLost));
            closing_fact.try_send(()).unwrap();
            assert_eq!(retirement.recv().await, Ok(42u64));
        }),
        None,
    );
    registry::send_control(shard.0, Control::SpawnService(Box::new(request))).unwrap();
    sim.kill_driver(shard, 1).unwrap(); // Public contract: the very next virtual wait.
    sim.run_until_idle();
    assert!(matches!(
        receipt.wait_blocking(),
        Ok(Admission::Admitted(_))
    ));
    assert_eq!(
        pending.try_recv(),
        Ok(()),
        "actual cancellation wait first Pending"
    );
    assert_eq!(
        closing.try_recv(),
        Ok(()),
        "loss reached the service's cleanup"
    );
    assert!(
        !sim.exited(shard).unwrap(),
        "pending ownership remains live"
    );
    assert_eq!(sim.live_tasks(shard), Ok(1));
    assert_eq!(
        sim.now_ns(),
        0,
        "terminal timer/kick metadata cannot advance time"
    );
    assert!(
        heard.try_recv().is_err(),
        "no forced resource Drop at quiescence"
    );
    release.try_send(42).unwrap();
    sim.run_until_idle();
    assert!(sim.exited(shard).unwrap());
    assert_eq!(sim.live_tasks(shard), Ok(0));
    let counts = sim.counters(shard).unwrap();
    assert_eq!(counts.driver_lost, 1);
    assert_eq!(counts.cancelled, 1);
    assert_eq!(counts.completed, 0);
    assert_eq!(heard.try_recv(), Ok(()));
    assert!(heard.try_recv().is_err());
    drop(sim);
    assert!(heard.try_recv().is_err(), "resource returned exactly once");
}
