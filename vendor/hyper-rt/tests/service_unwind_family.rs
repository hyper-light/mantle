//! Services finish ownership on their own task even when an unrelated poll unwinds,
//! or cancellation must first retire a cooperative child. No deadline releases ownership.
#![allow(
    clippy::cognitive_complexity,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::task::Poll;
use std::thread;

use hyper_rt::control::Control;
use hyper_rt::futures;
use hyper_rt::registry;
use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::sync;
use hyper_rt::task::{Admission, Outcome, SpawnRequest};

fn config(roles: usize) -> RuntimeConfig {
    // The existing terminal_admission clock settings do not decide event order or success.
    RuntimeConfig {
        shards: 1,
        tasks_per_shard: roles,
        timers_per_shard: roles,
        interests_per_shard: roles,
        ring_entries: roles,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: roles,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    }
}

struct Returned(SyncSender<&'static str>, &'static str);
impl Drop for Returned {
    fn drop(&mut self) {
        let _ = self.0.try_send(self.1);
    }
}

async fn observed<F: Future>(
    mut future: Pin<&mut F>,
    facts: &SyncSender<&'static str>,
    role: &'static str,
) -> F::Output {
    let mut noted = false;
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Pending => {
            if !noted {
                facts.try_send(role).unwrap();
                noted = true;
            }
            Poll::Pending
        }
        Poll::Ready(value) => Poll::Ready(value),
    })
    .await
}

#[test]
fn caught_foreign_poll_panic_does_not_strand_a_retained_peer_service() {
    // The service and the subsequently panicking root are the two admitted roles.
    let mut rt = LocalRuntime::new(&config(2)).unwrap();
    let (returned, heard) = sync_channel(2);
    let (pending_fact, pending) = sync_channel(1);
    let (closing_fact, closing) = sync_channel(1);
    let (release, mut completion) = sync::channel(1).unwrap();
    let capture = Returned(returned.clone(), "service");
    rt.spawn_service(async move {
        let _capture = capture;
        let mut cancelled = pin!(futures::cancelled());
        observed(cancelled.as_mut(), &pending_fact, "service pending")
            .await
            .unwrap();
        closing_fact.try_send(()).unwrap();
        assert_eq!(completion.recv().await, Ok(42u64));
    })
    .unwrap();
    rt.run_until_idle();
    assert_eq!(pending.try_recv(), Ok("service pending"));
    let ordinary = Returned(returned, "panicked root");
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on::<()>(async move {
            let _ordinary = ordinary;
            panic!("deliberate public foreign-future poll unwind");
        })
    }));
    assert!(panic.is_err());
    assert_eq!(heard.try_recv(), Ok("panicked root"));
    assert!(
        heard.try_recv().is_err(),
        "the peer service still owns its resource"
    );
    thread::scope(|scope| {
        let complete = scope.spawn(move || {
            closing.recv().unwrap();
            assert!(
                heard.try_recv().is_err(),
                "unwind dropped a live peer service"
            );
            release.blocking_send(42).unwrap();
            let returned = heard.recv().unwrap();
            assert!(heard.try_recv().is_err());
            returned
        });
        // The retained LocalRuntime is dropped after the catch, so thread::panicking
        // is false. The real interrupted-poll marker must close the missing root slot.
        drop(rt);
        assert_eq!(complete.join().unwrap(), "service");
    });
}

#[test]
fn cooperative_child_retires_before_parent_and_does_not_cancel_its_sibling() {
    // Parent, child, sibling, and a later independent progress task are the maximum
    // simultaneously occupied roles. The join root runs after every cleanup is done.
    let mut rt = LocalRuntime::new(&config(4)).unwrap();
    let (returned, heard) = sync_channel(3); // One resource for parent, child, sibling.
    let (first, pending) = sync_channel(3); // One actual first Pending per service.
    let (closing_fact, closing) = sync_channel(3); // One terminal-stage witness per service.
    let (parent_release, mut parent_done) = sync::channel(1).unwrap();
    let (child_release, mut child_done) = sync::channel(1).unwrap();
    let (sibling_release, mut sibling_done) = sync::channel(1).unwrap();
    let (child_retired, mut child_retirement) = sync::channel(1).unwrap();
    let parent_capture = Returned(returned.clone(), "parent");
    let parent_first = first.clone();
    let parent_closing = closing_fact.clone();
    let parent = rt
        .spawn_service(async move {
            let _capture = parent_capture;
            let mut cancel = pin!(futures::cancelled());
            observed(cancel.as_mut(), &parent_first, "parent pending")
                .await
                .unwrap();
            assert_eq!(child_retirement.recv().await, Ok("child retired"));
            parent_closing.try_send("parent cleanup").unwrap();
            assert_eq!(parent_done.recv().await, Ok(7u64));
        })
        .unwrap();
    let sibling_capture = Returned(returned.clone(), "sibling");
    let sibling_first = first.clone();
    let sibling_closing = closing_fact.clone();
    let sibling = rt
        .spawn_service(async move {
            let _capture = sibling_capture;
            let mut cancel = pin!(futures::cancelled());
            observed(cancel.as_mut(), &sibling_first, "sibling pending")
                .await
                .unwrap();
            sibling_closing.try_send("sibling cleanup").unwrap();
            assert_eq!(sibling_done.recv().await, Ok(11u64));
        })
        .unwrap();
    let child_capture = Returned(returned, "child");
    let (request, receipt) = SpawnRequest::with_receipt(
        Box::pin(async move {
            let _capture = child_capture;
            let mut cancel = pin!(futures::cancelled());
            observed(cancel.as_mut(), &first, "child pending")
                .await
                .unwrap();
            closing_fact.try_send("child cleanup").unwrap();
            assert_eq!(child_done.recv().await, Ok(13u64));
            child_retired.try_send("child retired").unwrap();
        }),
        Some(parent.0),
    );
    // Existing public structured-parent field plus the opt-in envelope; no private
    // task links, arena positions, or detached task-layout assertion is inspected.
    registry::send_control(rt.shard_id().0, Control::SpawnService(Box::new(request))).unwrap();
    rt.run_until_idle();
    assert!(matches!(
        receipt.wait_blocking(),
        Ok(Admission::Admitted(_))
    ));
    let mut initial = [
        pending.try_recv().unwrap(),
        pending.try_recv().unwrap(),
        pending.try_recv().unwrap(),
    ];
    initial.sort();
    assert_eq!(
        initial,
        ["child pending", "parent pending", "sibling pending"]
    );
    rt.context().cancel(parent).unwrap();
    rt.run_until_idle();
    assert_eq!(closing.try_recv(), Ok("child cleanup"));
    assert!(
        closing.try_recv().is_err(),
        "parent awaits child; sibling is not cancelled"
    );
    assert!(
        heard.try_recv().is_err(),
        "all physical-completion placeholders remain held"
    );
    let (progress, progressed) = sync_channel(1);
    let id = rt
        .spawn(async move {
            progress.try_send("independent task").unwrap();
        })
        .unwrap();
    rt.context().detach(id).unwrap();
    rt.run_until_idle();
    assert_eq!(progressed.try_recv(), Ok("independent task"));
    assert!(heard.try_recv().is_err());
    child_release.try_send(13).unwrap();
    rt.run_until_idle();
    assert_eq!(heard.try_recv(), Ok("child"));
    assert_eq!(closing.try_recv(), Ok("parent cleanup"));
    assert!(
        closing.try_recv().is_err(),
        "the unrelated sibling stays live"
    );
    assert!(
        heard.try_recv().is_err(),
        "parent still owns its completion"
    );
    parent_release.try_send(7).unwrap();
    rt.run_until_idle();
    assert_eq!(heard.try_recv(), Ok("parent"));
    rt.context().cancel(sibling).unwrap();
    rt.run_until_idle();
    assert_eq!(closing.try_recv(), Ok("sibling cleanup"));
    assert!(heard.try_recv().is_err());
    sibling_release.try_send(11).unwrap();
    rt.run_until_idle();
    assert_eq!(heard.try_recv(), Ok("sibling"));
    let joined =
        rt.block_on(async move { (futures::join(parent).await, futures::join(sibling).await) });
    assert_eq!(joined, Ok((Ok(Outcome::Cancelled), Ok(Outcome::Cancelled))));
    drop(rt);
    assert!(
        heard.try_recv().is_err(),
        "all captures returned exactly once"
    );
}
