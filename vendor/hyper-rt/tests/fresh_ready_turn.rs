//! A fixed poll budget serves new local handoffs in FIFO order without growing on self-wakes.
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
use hyper_rt::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::sync::channel as task_channel;

// No I/O or advancing clock is needed to prove the local FIFO handoff contract.
struct IdleDriver;

impl Driver for IdleDriver {
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
        Err(RtError::BadConfig {
            what: "the handoff fixture registers no native I/O",
        })
    }
    fn has_pending(&self) -> bool {
        false
    }
}

fn owner(roles: usize, polls_per_role: usize) -> LocalRuntime {
    let config = RuntimeConfig {
        shards: 1,
        tasks_per_shard: roles,
        timers_per_shard: roles,
        interests_per_shard: roles,
        ring_entries: roles,
        // The virtual driver never advances wall time; the configured poll count is the bound.
        step_budget_ns: 1,
        timer_tick_ns: 1,
        batch: roles.checked_mul(polls_per_role).unwrap(),
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    };
    let seed: DriverSeed = Box::new(|_| Ok(Box::new(IdleDriver) as Box<dyn Driver>));
    LocalRuntime::with_driver(&config, seed, Kick::None).unwrap()
}

#[derive(Debug, PartialEq, Eq)]
enum Fact {
    Poll(usize),
    Request(usize),
    Reply(usize),
    Dropped(usize),
}

struct Capture(usize, Sender<Fact>);
impl Drop for Capture {
    fn drop(&mut self) {
        let _ = self.1.send(Fact::Dropped(self.0));
    }
}

#[test]
fn ready_fifo_is_bounded_even_when_every_poll_requeues() {
    // Three ready roles, with room for two visits each in one phase.
    let roles = 3;
    let polls_per_role = 2;
    let mut rt = owner(roles, polls_per_role);
    let (facts, observed) = channel();
    let mut tasks = Vec::new();
    for role in 0..roles {
        let facts = facts.clone();
        let capture = Capture(role, facts.clone());
        tasks.push(
            rt.spawn(async move {
                let _capture = capture;
                loop {
                    facts.send(Fact::Poll(role)).unwrap();
                    hyper_rt::futures::yield_now().await;
                }
            })
            .unwrap(),
        );
    }
    assert!(rt.step().did_work);
    let first: Vec<_> = observed.try_iter().collect();
    assert!(rt.step().did_work);
    let second: Vec<_> = observed.try_iter().collect();
    for task in tasks {
        rt.context().cancel(task).unwrap();
    }
    rt.run_until_idle();
    let cleanup: Vec<_> = observed.try_iter().collect();
    drop(rt);
    // The verdict is retained until ordinary cancellation released every future.
    let expected: Vec<_> = (0..polls_per_role)
        .flat_map(|_| (0..roles).map(Fact::Poll))
        .collect();
    assert_eq!(
        first, expected,
        "fresh self-wakes use only the remaining configured phase budget"
    );
    assert_eq!(
        second, expected,
        "the next phase retains FIFO order and the same finite bound"
    );
    assert_eq!(cleanup.len(), roles);
    for role in 0..roles {
        assert!(cleanup.contains(&Fact::Dropped(role)));
    }
}

#[test]
fn local_request_reply_uses_remaining_budget_and_preserves_each_value() {
    // Client and actor, with three complete request/reply poll pairs per phase.
    let roles = 2;
    let pairs = 3;
    let rounds = roles * pairs;
    let mut rt = owner(roles, pairs);
    let (facts, observed) = channel();
    let (requests, mut offered) = task_channel(1).unwrap();
    let (answers, mut replies) = task_channel(1).unwrap();
    let client_facts = facts.clone();
    let client_capture = Capture(0, facts.clone());
    rt.spawn(async move {
        let _capture = client_capture;
        for value in 0..rounds {
            requests.try_send(value).unwrap();
            client_facts.send(Fact::Request(value)).unwrap();
            assert_eq!(replies.recv().await.unwrap(), value);
        }
    })
    .unwrap();
    let actor_capture = Capture(1, facts.clone());
    rt.spawn(async move {
        let _capture = actor_capture;
        for value in 0..rounds {
            assert_eq!(offered.recv().await.unwrap(), value);
            facts.send(Fact::Reply(value)).unwrap();
            answers.try_send(value).unwrap();
        }
    })
    .unwrap();
    assert!(rt.step().did_work);
    let first: Vec<_> = observed.try_iter().collect();
    rt.run_until_idle();
    let rest: Vec<_> = observed.try_iter().collect();
    drop(rt);
    let expected: Vec<_> = (0..pairs)
        .flat_map(|value| [Fact::Request(value), Fact::Reply(value)])
        .collect();
    assert_eq!(
        first, expected,
        "each actual channel handoff stays within this phase's poll budget"
    );
    let messages: Vec<_> = rest
        .iter()
        .filter(|fact| !matches!(fact, Fact::Dropped(_)))
        .collect();
    let later: Vec<_> = (pairs..rounds)
        .flat_map(|value| [Fact::Request(value), Fact::Reply(value)])
        .collect();
    assert_eq!(messages, later.iter().collect::<Vec<_>>());
    assert_eq!(
        rest.iter()
            .filter(|fact| matches!(fact, Fact::Dropped(_)))
            .count(),
        roles
    );
}
