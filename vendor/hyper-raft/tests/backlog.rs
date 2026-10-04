//! slates' backlog workload on this core (mantle note 32 §2.13, R21; `tests/support/backlog.rs`),
//! held exactly: what a proposal costs its leader in allocations, reallocations and bytes is the
//! same at a backlog of 1,000 entries its followers have not acknowledged as at 49,000. Its time is
//! measured by `benches/backlog.rs`.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cognitive_complexity,
    unreachable_pub
)]
mod support;

use hyper_measure::alloc::{self, Counting};
use support::backlog::Backlog;

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What `proposals` proposals from `first` cost the leader, the owner's storage set aside.
fn cost(backlog: &mut Backlog, first: u64, proposals: u64) -> alloc::Counts {
    alloc::begin();
    for at in first..first + proposals {
        backlog.propose(at);
    }
    let total = alloc::end();
    total.less(&alloc::read_aside())
}

#[test]
fn a_proposal_costs_the_leader_the_same_at_any_backlog() {
    assert!(
        alloc::installed(),
        "the counting allocator is not installed"
    );
    let mut backlog = Backlog::new(5);
    for at in 0..1_000 {
        backlog.propose(at);
    }
    let early = cost(&mut backlog, 1_000, 1_000);
    for at in 2_000..49_000 {
        backlog.propose(at);
    }
    let late = cost(&mut backlog, 49_000, 1_000);
    assert_eq!(backlog.backlog(), 50_001);
    assert_eq!(
        (early.allocations, early.reallocations, early.bytes),
        (late.allocations, late.reallocations, late.bytes),
        "a thousand proposals at a backlog of 1,000 and of 49,000"
    );
}
