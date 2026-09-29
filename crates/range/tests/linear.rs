//! The linearizability checker on histories whose verdict is known.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros
)]

mod support;

use support::linear::{Input, Operation, Output, Register, Verdict, check};

fn put(call: u64, ret: Option<u64>, value: &str) -> Operation<Input, Output> {
    Operation {
        call,
        ret,
        input: Input::Put(value.into()),
        output: ret.map(|_| Output::Put),
    }
}

fn get(call: u64, ret: u64, seen: Option<&str>) -> Operation<Input, Output> {
    Operation {
        call,
        ret: Some(ret),
        input: Input::Get,
        output: Some(Output::Got(seen.map(Into::into))),
    }
}

fn verdict(ops: &[Operation<Input, Output>]) -> Verdict {
    check(&Register, ops, 1_000_000)
}

#[test]
fn sequential_and_overlapping_histories_that_fit() {
    assert_eq!(verdict(&[]), Verdict::Linearizable);
    let sequential = [
        put(1, Some(2), "a"),
        get(3, 4, Some("a")),
        put(5, Some(6), "b"),
        get(7, 8, Some("b")),
    ];
    assert_eq!(verdict(&sequential), Verdict::Linearizable);
    // A get overlapping a put may see it or not.
    for seen in [None, Some("a")] {
        let overlapping = [put(1, Some(5), "a"), get(2, 3, seen)];
        assert_eq!(verdict(&overlapping), Verdict::Linearizable, "{seen:?}");
    }
    // Two overlapping puts in either order, as the gets that follow show.
    let racing = [
        put(1, Some(4), "a"),
        put(2, Some(3), "b"),
        get(5, 6, Some("a")),
        get(7, 8, Some("a")),
    ];
    assert_eq!(verdict(&racing), Verdict::Linearizable);
    // A put that never returned may have taken effect.
    let pending = [put(1, None, "c"), get(5, 6, Some("c"))];
    assert_eq!(verdict(&pending), Verdict::Linearizable);
    let pending_unseen = [put(1, None, "c"), get(5, 6, None)];
    assert_eq!(verdict(&pending_unseen), Verdict::Linearizable);
}

#[test]
fn histories_that_do_not_fit_are_refused() {
    // A get after a finished put must see it.
    let stale = [put(1, Some(2), "a"), get(3, 4, None)];
    assert!(matches!(verdict(&stale), Verdict::NotLinearizable { .. }));
    // A later put finished before the get began, so it must see the later value.
    let lost = [
        put(1, Some(2), "a"),
        put(3, Some(4), "b"),
        get(5, 6, Some("a")),
    ];
    assert!(matches!(verdict(&lost), Verdict::NotLinearizable { .. }));
    // Two gets in real-time order cannot see the writes in opposite orders.
    let flipped = [
        put(1, Some(10), "a"),
        put(1, Some(10), "b"),
        get(2, 3, Some("a")),
        get(4, 5, Some("b")),
        get(6, 7, Some("a")),
    ];
    assert!(matches!(verdict(&flipped), Verdict::NotLinearizable { .. }));
    // A value nobody wrote.
    let invented = [get(1, 2, Some("z"))];
    assert!(matches!(
        verdict(&invented),
        Verdict::NotLinearizable { .. }
    ));
}
