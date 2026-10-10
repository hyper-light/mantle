//! Manual public park must stop using the driver if applying an initial interest loses it.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::future::pending;
use std::sync::mpsc::{Sender, channel};
use std::task::Poll;

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, Kick};
use hyper_rt::interests::Readiness;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::task::Outcome;

#[derive(Debug, PartialEq, Eq)]
enum DeviceEvent {
    Arm(i32),
    Wait,
    Disarm(i32),
    TaskDropped,
}

struct LostOnArm(Sender<DeviceEvent>);

impl Driver for LostOnArm {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }
    fn kick_handle(&self) -> Kick {
        Kick::None
    }
    fn now_ns(&self) -> u64 {
        0
    }
    fn wait(&mut self, _timeout: Option<u64>, _out: &mut Vec<Completion>) -> Result<(), RtError> {
        let _ = self.0.send(DeviceEvent::Wait);
        Err(RtError::DriverLost)
    }
    fn submit_nop(&mut self, _word: u64) -> Result<(), RtError> {
        Ok(())
    }
    fn arm(&mut self, raw: i32, _want: Readiness, _tag: u64) -> Result<(), RtError> {
        let _ = self.0.send(DeviceEvent::Arm(raw));
        Err(RtError::DriverLost)
    }
    fn disarm(&mut self, raw: i32) {
        let _ = self.0.send(DeviceEvent::Disarm(raw));
    }
    fn has_pending(&self) -> bool {
        false
    }
}

struct TaskCapture(Sender<DeviceEvent>);

impl Drop for TaskCapture {
    fn drop(&mut self) {
        let _ = self.0.send(DeviceEvent::TaskDropped);
    }
}

#[test]
fn initial_arm_loss_during_park_cancels_the_task_without_another_device_operation() {
    // Existing lifecycle shape; the two handles below are adapter identifiers, never OS fds.
    let config = RuntimeConfig {
        shards: 1,
        tasks_per_shard: 16,
        timers_per_shard: 16,
        interests_per_shard: 64,
        ring_entries: 16,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: 16,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    };
    let (events, observed) = channel();
    let capture = TaskCapture(events.clone());
    let mut rt = LocalRuntime::with_driver(
        &config,
        Box::new(move |_kick| Ok(Box::new(LostOnArm(events)) as Box<dyn Driver>)),
        Kick::None,
    )
    .unwrap();
    let task = rt
        .spawn(async move {
            let _capture = capture;
            pending::<()>().await;
        })
        .unwrap();
    rt.context().register_interest(0, false, task.0).unwrap();
    rt.context().register_interest(1, true, task.0).unwrap();
    rt.park(None);
    // These are the public device adapter's operations and a captured object's destruction,
    // observed before owner Drop could hide a still-live task or a dead-driver operation.
    let before_drop: Vec<_> = observed.try_iter().collect();
    let joined = rt.context().poll_join(task, None);
    drop(rt);
    assert_eq!(before_drop, [DeviceEvent::Arm(0), DeviceEvent::TaskDropped]);
    assert_eq!(joined, Poll::Ready(Ok(Outcome::Cancelled)));
}
