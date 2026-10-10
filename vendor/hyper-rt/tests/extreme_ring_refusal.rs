//! Impossible public ring capacity is refused normally before it can panic or strand the owner.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, Kick};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};

struct Healthy;
impl Driver for Healthy {
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
        Ok(())
    }
    fn submit_nop(&mut self, _word: u64) -> Result<(), RtError> {
        Ok(())
    }
    fn arm(
        &mut self,
        _raw: i32,
        _want: hyper_rt::interests::Readiness,
        _tag: u64,
    ) -> Result<(), RtError> {
        Ok(())
    }
    fn has_pending(&self) -> bool {
        false
    }
}

fn config() -> RuntimeConfig {
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: 4,
        timers_per_shard: 4,
        interests_per_shard: 8,
        ring_entries: 4,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: 4,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

fn make(config: &RuntimeConfig) -> Result<LocalRuntime, RtError> {
    LocalRuntime::with_driver(
        config,
        Box::new(|_kick| Ok(Box::new(Healthy) as Box<dyn Driver>)),
        Kick::None,
    )
}

#[test]
fn an_impossible_public_ring_is_a_typed_refusal_then_a_healthy_owner_still_works() {
    let mut extreme = config();
    extreme.ring_entries = usize::MAX;
    let refusal = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| make(&extreme)));
    let mut healthy = make(&config()).expect("healthy owner after refused setup");
    let exact = healthy.block_on(async { 42 });
    drop(healthy);
    assert!(
        matches!(refusal, Ok(Err(_))),
        "impossible capacity must be refused without unwinding"
    );
    assert_eq!(exact, Ok(42));
}
