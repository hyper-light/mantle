//! The same public lifetime oracle builds on the measured baseline and the candidate.
//! The test-only extension names the desired admission on the baseline; the candidate's
//! inherent method wins Rust method resolution. No production compatibility shim ships.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc,
    dead_code
)]

use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
use hyper_rt::{RtError, TaskId};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::mpsc::sync_channel;
use std::task::Poll;

trait BaselineServiceAdmission {
    fn spawn_service<F: Future<Output = ()> + 'static>(&mut self, f: F) -> Result<TaskId, RtError>;
}

impl BaselineServiceAdmission for LocalRuntime {
    fn spawn_service<F: Future<Output = ()> + 'static>(&mut self, f: F) -> Result<TaskId, RtError> {
        self.spawn(f)
    }
}

struct Returned(std::sync::mpsc::SyncSender<()>);
impl Drop for Returned {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

#[test]
fn an_opted_service_retains_ownership_until_its_completion() {
    // Two roles: the service and the later join root. Clock values reuse the existing
    // terminal_admission fixture and never select a success or cancellation outcome.
    let cfg = RuntimeConfig {
        shards: 1,
        tasks_per_shard: 2,
        timers_per_shard: 2,
        interests_per_shard: 2,
        ring_entries: 2,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        batch: 2,
        pin: false,
        cores: Vec::new(),
        page_bytes: 4096,
        spin_ns: 0,
        wake_tracking: None,
    };
    let (returned, heard) = sync_channel(1);
    let (first, pending) = sync_channel(1);
    let (release, mut completion) = hyper_rt::sync::channel(1).unwrap();
    let capture = Returned(returned);
    let mut rt = LocalRuntime::new(&cfg).unwrap();
    let service = rt
        .spawn_service(async move {
            let _capture = capture;
            let mut ready = pin!(completion.recv());
            let mut noted = false;
            let value = poll_fn(|cx| match ready.as_mut().poll(cx) {
                Poll::Pending => {
                    if !noted {
                        first.try_send(()).unwrap();
                        noted = true;
                    }
                    Poll::Pending
                }
                Poll::Ready(value) => Poll::Ready(value),
            })
            .await
            .unwrap();
            assert_eq!(value, 42u64);
        })
        .unwrap();
    rt.run_until_idle();
    assert_eq!(
        pending.try_recv(),
        Ok(()),
        "the service really owns a Pending completion"
    );
    rt.context().cancel(service).unwrap();
    rt.run_until_idle();
    let premature = heard.try_recv().is_ok();
    // Cleanup happens before the verdict even on baseline failure. The baseline may
    // have closed the receiver, which is itself a consequence of premature Drop.
    let sent = release.try_send(42);
    rt.run_until_idle();
    let joined = rt.block_on(async move { hyper_rt::futures::join(service).await });
    let final_return = if premature {
        true
    } else {
        heard.try_recv().is_ok()
    };
    drop(rt);
    assert!(
        !premature,
        "cancellation dropped external ownership before completion"
    );
    assert_eq!(sent, Ok(()));
    assert_eq!(joined, Ok(Ok(hyper_rt::task::Outcome::Cancelled)));
    assert!(final_return, "terminal completion returned the resource");
    assert!(heard.try_recv().is_err(), "resource returned exactly once");
}
