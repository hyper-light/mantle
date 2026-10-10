//! Driver loss is terminal in the public nonblocking step path, including partial completion.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc,
    clippy::cognitive_complexity
)]

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::mpsc::{Sender, channel};
use std::task::Poll;

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, Kick};
use hyper_rt::interests::Readiness;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::task::Outcome;

const ROLES: usize = 2;
// A public fault adapter identifier, never an operating-system descriptor.
const HANDLE: i32 = 0;

#[derive(Clone, Copy)]
enum Fault {
    InitialArm,
    Retrieval,
    PartialRead,
}

#[derive(Debug, PartialEq, Eq)]
enum DeviceCall {
    Arm,
    Wait(Option<u64>),
    Disarm,
}

struct FaultDriver {
    fault: Fault,
    tag: Option<u64>,
    calls: Sender<DeviceCall>,
}

impl Driver for FaultDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }
    fn kick_handle(&self) -> Kick {
        Kick::None
    }
    fn now_ns(&self) -> u64 {
        0
    }
    fn wait(&mut self, timeout: Option<u64>, out: &mut Vec<Completion>) -> Result<(), RtError> {
        let _ = self.calls.send(DeviceCall::Wait(timeout));
        if matches!(self.fault, Fault::PartialRead)
            && let Some(tag) = self.tag
        {
            out.push(Completion {
                user_data: tag,
                result: Readiness::READ.bits(),
            });
        }
        Err(RtError::DriverLost)
    }
    fn submit_nop(&mut self, _word: u64) -> Result<(), RtError> {
        Ok(())
    }
    fn arm(&mut self, raw: i32, want: Readiness, tag: u64) -> Result<(), RtError> {
        let _ = self.calls.send(DeviceCall::Arm);
        if raw != HANDLE || want != Readiness::READ {
            return Err(RtError::BadConfig {
                what: "one read target in the public step fault adapter",
            });
        }
        self.tag = Some(tag);
        if matches!(self.fault, Fault::InitialArm) {
            Err(RtError::DriverLost)
        } else {
            Ok(())
        }
    }
    fn disarm(&mut self, _raw: i32) {
        let _ = self.calls.send(DeviceCall::Disarm);
    }
    fn has_pending(&self) -> bool {
        false
    }
}

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: ROLES,
        timers_per_shard: ROLES,
        interests_per_shard: ROLES,
        ring_entries: ROLES,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: ROLES,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

#[derive(Debug)]
enum Fact {
    FirstPending,
    Finished(Result<(), RtError>),
    Dropped(&'static str),
}

struct Capture(&'static str, Sender<Fact>);

impl Drop for Capture {
    fn drop(&mut self) {
        let _ = self.1.send(Fact::Dropped(self.0));
    }
}

fn loss_case(fault: Fault) {
    let config = config();
    let turns = config.tasks_per_shard.checked_add(1).unwrap();
    let (calls, device_calls) = channel();
    let mut rt = LocalRuntime::with_driver(
        &config,
        Box::new(move |_kick| {
            Ok(Box::new(FaultDriver {
                fault,
                tag: None,
                calls,
            }) as Box<dyn Driver>)
        }),
        Kick::None,
    )
    .unwrap();
    let (events, observed) = channel();
    let root_capture = Capture("read", events.clone());
    let report = events.clone();
    let read_task = rt
        .spawn(async move {
            let _capture = root_capture;
            let mut read = pin!(hyper_rt::readiness::readable(HANDLE));
            let first = poll_fn(|cx| {
                Poll::Ready(match read.as_mut().poll(cx) {
                    Poll::Pending => {
                        let _ = report.send(Fact::FirstPending);
                        Ok(())
                    }
                    Poll::Ready(result) => result,
                })
            })
            .await;
            let result = match first {
                Ok(()) => read.await,
                Err(error) => Err(error),
            };
            let _ = report.send(Fact::Finished(result));
        })
        .unwrap();
    let busy_capture = Capture("busy", events);
    let busy_task = rt
        .spawn(async move {
            let _capture = busy_capture;
            loop {
                hyper_rt::futures::yield_now().await;
            }
        })
        .unwrap();
    let mut exited = false;
    for _ in 0..turns {
        if rt.step().exit {
            exited = true;
            break;
        }
    }
    // Terminal cleanup and join outcomes must hold before owner Drop can hide a leak.
    let before_drop: Vec<_> = observed.try_iter().collect();
    let joined_read = rt.context().poll_join(read_task, None);
    let joined_busy = rt.context().poll_join(busy_task, None);
    let mut dropped: Vec<_> = before_drop
        .iter()
        .filter_map(|fact| match fact {
            Fact::Dropped(role) => Some(*role),
            _ => None,
        })
        .collect();
    dropped.sort_unstable();
    drop(rt);
    let actual_calls: Vec<_> = device_calls.try_iter().collect();
    let expected_calls = if matches!(fault, Fault::InitialArm) {
        vec![DeviceCall::Arm]
    } else {
        vec![DeviceCall::Arm, DeviceCall::Wait(Some(0))]
    };
    assert!(
        before_drop
            .iter()
            .any(|fact| matches!(fact, Fact::FirstPending))
    );
    assert!(exited, "public step must report terminal driver loss");
    assert_eq!(joined_read, Poll::Ready(Ok(Outcome::Cancelled)));
    assert_eq!(joined_busy, Poll::Ready(Ok(Outcome::Cancelled)));
    assert_eq!(dropped, ["busy", "read"]);
    assert!(
        !before_drop
            .iter()
            .any(|fact| matches!(fact, Fact::Finished(Ok(())))),
        "partial completion cannot win over fatal loss: {before_drop:?}"
    );
    assert_eq!(
        actual_calls, expected_calls,
        "no later use of the lost adapter, including retirement"
    );
}

#[test]
fn manual_step_initial_arm_loss_cancels_both_tasks_without_later_device_use() {
    loss_case(Fault::InitialArm);
}

#[test]
fn manual_step_retrieval_loss_cancels_both_tasks_without_later_device_use() {
    loss_case(Fault::Retrieval);
}

#[test]
fn manual_step_partial_read_then_loss_cannot_complete_the_read() {
    loss_case(Fault::PartialRead);
}
