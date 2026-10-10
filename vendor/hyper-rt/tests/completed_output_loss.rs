//! A root's completed output is returned to its caller or destroyed before a failed block_on returns.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::sync::mpsc::{Sender, channel};

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, Kick};
use hyper_rt::interests::Readiness;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

struct LostOnArm(Sender<()>);

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
        Err(RtError::DriverLost)
    }
    fn submit_nop(&mut self, _word: u64) -> Result<(), RtError> {
        Ok(())
    }
    fn arm(&mut self, _raw: i32, _want: Readiness, _tag: u64) -> Result<(), RtError> {
        let _ = self.0.send(());
        Err(RtError::DriverLost)
    }
    fn has_pending(&self) -> bool {
        false
    }
}

#[derive(Debug)]
struct Output {
    value: u32,
    dropped: Sender<u32>,
}

impl Drop for Output {
    fn drop(&mut self) {
        let _ = self.dropped.send(self.value);
    }
}

#[test]
fn completed_output_is_not_retained_by_the_owner_after_a_driver_loss_error() {
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
    let (lost, faults) = channel();
    let (dropped, notices) = channel();
    let mut rt = LocalRuntime::with_driver(
        &config,
        Box::new(move |_kick| Ok(Box::new(LostOnArm(lost)) as Box<dyn Driver>)),
        Kick::None,
    )
    .unwrap();
    let output = Output { value: 42, dropped };
    let result = rt.block_on(async move {
        let task =
            hyper_rt::futures::current_task().expect("the root owns its public task context");
        // A public interest queued by this poll is applied after its returned T was produced.
        hyper_rt::registry::with_current(|ctx| ctx.register_interest(0, false, task.0))
            .unwrap()
            .unwrap();
        output
    });
    let arm_lost = faults.try_recv().is_ok();
    let policy_valid = match result {
        Ok(output) => {
            let caller_owned = output.value == 42 && notices.try_recv().is_err();
            drop(output);
            caller_owned
        }
        Err(RtError::ShardGone { .. }) => true,
        Err(_) => false,
    };
    // An Err must have destroyed T already; an Ok must leave that ownership with the caller.
    let returned_before_owner_drop: Vec<_> = notices.try_iter().collect();
    drop(rt);
    let additional_after_owner_drop: Vec<_> = notices.try_iter().collect();
    assert!(
        arm_lost,
        "the public adapter actually returned DriverLost while applying the root's interest"
    );
    assert!(
        policy_valid,
        "completed-value/fatal-step precedence must retain correct ownership"
    );
    assert_eq!(returned_before_owner_drop, [42]);
    assert!(
        additional_after_owner_drop.is_empty(),
        "the value is destroyed exactly once"
    );
}
