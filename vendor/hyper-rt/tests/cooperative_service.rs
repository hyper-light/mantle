//! Public service lifetime tests. Completion data, actual Pending, and parked-owner
//! facts decide success. The external test supervisor's deadline is failure only.
//! These are runtime ownership/wake oracles; actual disk quiescence is a separate
//! Mantle held-issuer test, not inferred from a channel value here.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::missing_panics_doc
)]

use std::future::{Future, pending, poll_fn};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::task::Poll;
use std::thread;

use hyper_rt::RtError;
use hyper_rt::driver::{Completion, Driver, DriverKind, DriverSeed, Kick};
use hyper_rt::futures;
use hyper_rt::interests::Readiness;
use hyper_rt::registry;
use hyper_rt::runtime::{LocalRuntime, Runtime, RuntimeConfig, submit_service_to_holder};
use hyper_rt::sync;
use hyper_rt::task::{Admission, Outcome};

/// One service, its controlling root, and a separate progress task.
const ROLES: usize = 3;
/// An opaque handle used only by the fault adapter, never by the OS.
const HANDLE: i32 = 0;
/// One readiness service and one continuously ready cleanup service.
static UDP_DELIVERED: AtomicBool = AtomicBool::new(false);
static UDP_BUSY_STARTED: AtomicBool = AtomicBool::new(false);

fn config() -> RuntimeConfig {
    // Clock values reuse the existing terminal_admission lifecycle fixture. They
    // do not control an outcome, watchdog, retry bound, or event order.
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

struct Returned(SyncSender<&'static str>, &'static str);

impl Drop for Returned {
    fn drop(&mut self) {
        let _ = self.0.try_send(self.1);
    }
}

async fn first_pending<F: Future>(mut future: Pin<&mut F>, fact: &sync::Sender<()>) -> F::Output {
    let mut sent = false;
    poll_fn(|cx| match future.as_mut().poll(cx) {
        Poll::Ready(value) => Poll::Ready(value),
        Poll::Pending => {
            if !sent {
                fact.try_send(()).expect("the single fact has its own slot");
                sent = true;
            }
            Poll::Pending
        }
    })
    .await
}

fn no_returned_resource(receiver: &Receiver<&'static str>) {
    assert!(
        receiver.try_recv().is_err(),
        "ownership returned before completion"
    );
}

#[test]
fn ordinary_tasks_keep_their_existing_hard_cancel_contract() {
    let (returned, heard) = sync_channel(1);
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let capture = Returned(returned, "ordinary");
    let id = rt
        .spawn(async move {
            let _capture = capture;
            pending::<()>().await;
        })
        .unwrap();
    rt.context().cancel(id).unwrap();
    assert_eq!(
        rt.block_on(async move { futures::join(id).await }),
        Ok(Ok(Outcome::Cancelled))
    );
    assert_eq!(heard.try_recv(), Ok("ordinary"));
    assert_eq!(rt.block_on(async { "same owner" }), Ok("same owner"));
}

#[test]
fn a_cancelled_service_keeps_ownership_while_a_sibling_makes_progress() {
    let (returned, heard) = sync_channel(1);
    let (pending_fact, mut first) = sync::channel(1).unwrap();
    let (closing_fact, mut closing) = sync::channel(1).unwrap();
    let (release, mut retirement) = sync::channel(1).unwrap();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    rt.block_on(async move {
        let capture = Returned(returned, "service");
        let id = futures::spawn_service(async move {
            let _capture = capture;
            let mut cancel = pin!(futures::cancelled());
            first_pending(cancel.as_mut(), &pending_fact).await.unwrap();
            closing_fact.try_send(()).unwrap();
            assert_eq!(retirement.recv().await, Ok(42u64));
        })
        .unwrap();
        first.recv().await.unwrap();
        futures::cancel(id).unwrap();
        closing.recv().await.unwrap();
        no_returned_resource(&heard);
        let (progress, mut progressed) = sync::channel(1).unwrap();
        let sibling = futures::spawn(async move {
            progress.try_send("sibling").unwrap();
        })
        .unwrap();
        assert_eq!(progressed.recv().await, Ok("sibling"));
        assert_eq!(futures::join(sibling).await, Ok(Outcome::Completed));
        no_returned_resource(&heard);
        release.try_send(42).unwrap();
        assert_eq!(futures::join(id).await, Ok(Outcome::Cancelled));
        assert_eq!(heard.try_recv(), Ok("service"));
        assert!(heard.try_recv().is_err());
    })
    .unwrap();
    assert_eq!(rt.block_on(async { "healthy reuse" }), Ok("healthy reuse"));
}

#[test]
fn service_cancel_before_first_poll_still_runs_its_cleanup() {
    let (returned, heard) = sync_channel(1);
    let (closing_fact, mut closing) = sync::channel(1).unwrap();
    let (release, mut retirement) = sync::channel(1).unwrap();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let capture = Returned(returned, "before first poll");
    let id = rt
        .spawn_service(async move {
            let _capture = capture;
            futures::cancelled().await.unwrap();
            closing_fact.try_send(()).unwrap();
            assert_eq!(retirement.recv().await, Ok(7u64));
        })
        .unwrap();
    rt.context().cancel(id).unwrap();
    rt.block_on(async move {
        closing.recv().await.unwrap();
        no_returned_resource(&heard);
        release.try_send(7).unwrap();
        assert_eq!(futures::join(id).await, Ok(Outcome::Cancelled));
        assert_eq!(heard.try_recv(), Ok("before first poll"));
    })
    .unwrap();
}

#[test]
fn owner_drop_waits_for_service_retirement_without_forcing_future_drop() {
    let (returned, heard) = sync_channel(1);
    let (closing_fact, closing) = sync_channel(1);
    let (release, mut retirement) = sync::channel(1).unwrap();
    let mut rt = LocalRuntime::new(&config()).unwrap();
    let capture = Returned(returned, "owner drop");
    rt.spawn_service(async move {
        let _capture = capture;
        futures::cancelled().await.unwrap();
        closing_fact.try_send(()).unwrap();
        assert_eq!(retirement.recv().await, Ok(11u64));
    })
    .unwrap();
    thread::scope(|scope| {
        let releaser = scope.spawn(move || {
            closing.recv().unwrap();
            no_returned_resource(&heard);
            release.blocking_send(11).unwrap();
            heard.recv().unwrap()
        });
        drop(rt);
        assert_eq!(releaser.join().unwrap(), "owner drop");
    });
}

#[test]
fn threaded_shutdown_keeps_an_actually_parked_service_until_completion() {
    let (pending_fact, first) = sync_channel(1);
    let (closing_fact, closing) = sync_channel(1);
    let (returned, heard) = sync_channel(1);
    let (release, mut retirement) = sync::channel(1).unwrap();
    let rt = Runtime::start(&config()).unwrap();
    let shard = rt.shard_ids().first().copied().unwrap();
    let capture = Returned(returned, "threaded stop");
    let receipt = rt
        .spawn_service_on_with_receipt(shard, async move {
            let _capture = capture;
            let mut cancel = pin!(futures::cancelled());
            let mut fact = false;
            poll_fn(|cx| match cancel.as_mut().poll(cx) {
                Poll::Pending => {
                    if !fact {
                        pending_fact.try_send(()).unwrap();
                        fact = true;
                    }
                    Poll::Pending
                }
                Poll::Ready(value) => Poll::Ready(value),
            })
            .await
            .unwrap();
            closing_fact.try_send(()).unwrap();
            assert_eq!(retirement.recv().await, Ok(13u64));
        })
        .unwrap();
    assert!(matches!(
        receipt.wait_blocking(),
        Ok(Admission::Admitted(_))
    ));
    first.recv().unwrap();
    thread::scope(|scope| {
        let releaser = scope.spawn(move || {
            closing.recv().unwrap();
            while !registry::with_entry(shard.0, |entry| entry.parking.parked()).unwrap_or(false) {
                thread::yield_now();
            }
            no_returned_resource(&heard);
            release.blocking_send(13).unwrap();
            heard.recv().unwrap()
        });
        assert!(rt.shutdown().is_ok());
        assert_eq!(releaser.join().unwrap(), "threaded stop");
    });
}

struct LostOnArm {
    lost: bool,
    first_healthy: bool,
    armed: bool,
    calls: SyncSender<&'static str>,
}

impl Driver for LostOnArm {
    fn kind(&self) -> DriverKind {
        DriverKind::Simulation
    }
    fn kick_handle(&self) -> Kick {
        Kick::None
    }
    fn now_ns(&self) -> u64 {
        if self.lost {
            let _ = self.calls.try_send("clock after loss");
        }
        0
    }
    fn wait(&mut self, _: Option<u64>, _: &mut Vec<Completion>) -> Result<(), RtError> {
        if self.lost {
            let _ = self.calls.try_send("wait after loss");
        }
        Ok(())
    }
    fn submit_nop(&mut self, _: u64) -> Result<(), RtError> {
        if self.lost {
            let _ = self.calls.try_send("submit after loss");
        }
        Ok(())
    }
    fn arm(&mut self, _: i32, _: Readiness, _: u64) -> Result<(), RtError> {
        if self.first_healthy && !self.armed {
            self.armed = true;
            let _ = self.calls.try_send("arm accepts waiting service");
            return Ok(());
        }
        let _ = self.calls.try_send(if self.lost {
            "arm after loss"
        } else {
            "arm loses driver"
        });
        self.lost = true;
        Err(RtError::DriverLost)
    }
    fn disarm(&mut self, _: i32) {
        if self.lost {
            let _ = self.calls.try_send("disarm after loss");
        }
    }
    fn has_pending(&self) -> bool {
        if self.lost {
            let _ = self.calls.try_send("has_pending after loss");
        }
        false
    }
}

#[test]
fn driver_loss_uses_channel_wakes_after_actual_cleanup_park() {
    // Enough room for each adapter method's first misuse and the expected initial arm.
    let (calls, called) = sync_channel(7);
    let seed: DriverSeed = Box::new(move |_| {
        Ok(Box::new(LostOnArm {
            lost: false,
            first_healthy: false,
            armed: false,
            calls,
        }))
    });
    let mut rt = LocalRuntime::with_driver(&config(), seed, Kick::None).unwrap();
    let shard = rt.shard_id();
    let (closing_fact, closing) = sync_channel(1);
    let (returned, heard) = sync_channel(1);
    let (release, mut retirement) = sync::channel(1).unwrap();
    let capture = Returned(returned, "driver loss");
    rt.spawn_service(async move {
        let _capture = capture;
        futures::cancelled().await.unwrap();
        assert_eq!(
            futures::sleep(config().timer_tick_ns).await,
            Err(RtError::DriverLost)
        );
        assert_eq!(
            hyper_rt::readiness::readable(HANDLE).await,
            Err(RtError::DriverLost)
        );
        closing_fact.try_send(()).unwrap();
        assert_eq!(retirement.recv().await, Ok(17u64));
    })
    .unwrap();
    thread::scope(|scope| {
        let releaser = scope.spawn(move || {
            closing.recv().unwrap();
            while !registry::with_entry(shard.0, |entry| entry.parking.parked()).unwrap_or(false) {
                thread::yield_now();
            }
            no_returned_resource(&heard);
            release.blocking_send(17).unwrap();
            heard.recv().unwrap()
        });
        let result = rt.block_on(hyper_rt::readiness::readable(HANDLE));
        assert!(
            matches!(result, Err(RtError::ShardGone { .. })),
            "{result:?}"
        );
        assert_eq!(releaser.join().unwrap(), "driver loss");
    });
    assert_eq!(called.try_recv(), Ok("arm loses driver"));
    assert!(called.try_recv().is_err(), "the dead driver was used again");
    drop(rt);
    assert!(
        called.try_recv().is_err(),
        "owner retirement used the dead driver"
    );
}

#[test]
fn queued_and_refused_bootstraps_answer_their_actual_admission_fate() {
    let (returned, heard) = sync_channel(1);
    let rt = LocalRuntime::new(&config()).unwrap();
    let holder = registry::holder_of(rt.shard_id().0).unwrap();
    let cold = Returned(returned, "unadmitted bootstrap");
    let receipt = submit_service_to_holder(holder, async move {
        let _cold = cold;
        pending::<()>().await;
    })
    .unwrap();
    // This queued future deliberately owns only cold test setup, never live I/O.
    drop(rt);
    assert_eq!(receipt.wait_blocking(), Ok(Admission::Terminated));
    assert_eq!(heard.try_recv(), Ok("unadmitted bootstrap"));

    let mut cfg = config();
    // One occupied arena slot provides the exact public capacity-refusal witness.
    cfg.tasks_per_shard = 1;
    let mut rt = LocalRuntime::new(&cfg).unwrap();
    let old = rt.spawn(pending()).unwrap();
    let holder = registry::holder_of(rt.shard_id().0).unwrap();
    let (returned, heard) = sync_channel(1);
    let cold = Returned(returned, "refused bootstrap");
    let receipt = submit_service_to_holder(holder, async move {
        let _cold = cold;
        pending::<()>().await;
    })
    .unwrap();
    rt.step();
    assert!(matches!(
        receipt.wait_blocking(),
        Ok(Admission::Refused(RtError::TooManyTasks { capacity: 1 }))
    ));
    assert_eq!(heard.try_recv(), Ok("refused bootstrap"));
    rt.context().detach(old).unwrap();
    rt.context().cancel(old).unwrap();
    rt.run_until_idle();
    assert_eq!(
        rt.block_on(async { "reuse after refusal" }),
        Ok("reuse after refusal")
    );
}

#[test]
fn an_already_armed_service_wait_returns_driver_loss_before_retirement() {
    // Two expected arms plus the six distinct forbidden post-loss adapter methods.
    let (calls, called) = sync_channel(8);
    let seed: DriverSeed = Box::new(move |_| {
        Ok(Box::new(LostOnArm {
            lost: false,
            first_healthy: true,
            armed: false,
            calls,
        }))
    });
    let mut rt = LocalRuntime::with_driver(&config(), seed, Kick::None).unwrap();
    let shard = rt.shard_id();
    let (first, mut pending_fact) = sync::channel(1).unwrap();
    let (closing_fact, closing) = sync_channel(1);
    let (returned, heard) = sync_channel(1);
    let (release, mut retirement) = sync::channel(1).unwrap();
    let capture = Returned(returned, "armed readiness loss");
    rt.spawn_service(async move {
        let _capture = capture;
        // Keep this exact future across loss; constructing another Ready would not
        // exercise the retained ticket's terminal result.
        let mut ready = pin!(hyper_rt::readiness::readable(HANDLE));
        assert_eq!(
            first_pending(ready.as_mut(), &first).await,
            Err(RtError::DriverLost)
        );
        closing_fact.try_send(()).unwrap();
        assert_eq!(retirement.recv().await, Ok(19u64));
    })
    .unwrap();
    thread::scope(|scope| {
        let releaser = scope.spawn(move || {
            closing.recv().unwrap();
            while !registry::with_entry(shard.0, |entry| entry.parking.parked()).unwrap_or(false) {
                thread::yield_now();
            }
            no_returned_resource(&heard);
            release.blocking_send(19).unwrap();
            heard.recv().unwrap()
        });
        let result = rt.block_on(async move {
            pending_fact.recv().await.unwrap();
            // The first service poll's registration was applied before another
            // task runs. The adapter accepts it; this distinct second handle loses it.
            hyper_rt::readiness::readable(HANDLE.saturating_add(1)).await
        });
        assert!(
            matches!(result, Err(RtError::ShardGone { .. })),
            "{result:?}"
        );
        assert_eq!(releaser.join().unwrap(), "armed readiness loss");
    });
    assert_eq!(called.try_recv(), Ok("arm accepts waiting service"));
    assert_eq!(called.try_recv(), Ok("arm loses driver"));
    assert!(called.try_recv().is_err());
    drop(rt);
    assert!(called.try_recv().is_err());
}

#[test]
#[cfg_attr(miri, ignore)] // This oracle uses the native driver and a real loopback receive queue.
fn healthy_shutdown_harvests_native_readiness_while_another_cleanup_is_busy() {
    const PAYLOAD: &[u8] = b"service shutdown readiness";
    UDP_DELIVERED.store(false, Ordering::Release);
    UDP_BUSY_STARTED.store(false, Ordering::Release);
    let observed = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    observed.set_nonblocking(true).unwrap();
    let socket = hyper_rt::udp::UdpSocket::adopt(observed.try_clone().unwrap().into()).unwrap();
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    peer.set_nonblocking(true).unwrap();
    let address = observed.local_addr().unwrap();
    let (pending, first) = sync_channel(2); // One real cancellation Pending per service.
    let (io_pending, io_first) = sync_channel(1);
    let (returned, heard) = sync_channel(2); // One exact terminal capture per service.
    let rt = Runtime::start(&config()).unwrap();
    let shard = rt.shard_ids().first().copied().unwrap();
    let waiting = pending.clone();
    let capture = Returned(returned.clone(), "readiness cleanup");
    let receipt = rt
        .spawn_service_on_with_receipt(shard, async move {
            let _capture = capture;
            let mut cancel = pin!(futures::cancelled());
            let mut sent = false;
            poll_fn(|cx| match cancel.as_mut().poll(cx) {
                Poll::Pending => {
                    if !sent {
                        waiting.try_send("reader").unwrap();
                        sent = true;
                    }
                    Poll::Pending
                }
                Poll::Ready(value) => Poll::Ready(value),
            })
            .await
            .unwrap();
            let mut ready = pin!(socket.readable());
            poll_fn(|cx| {
                Poll::Ready(match ready.as_mut().poll(cx) {
                    Poll::Pending => Ok(()),
                    Poll::Ready(value) => {
                        Err(format!("empty cleanup socket did not wait: {value:?}"))
                    }
                })
            })
            .await
            .unwrap();
            io_pending.try_send(()).unwrap();
            ready.await.unwrap();
            let mut received = [0; PAYLOAD.len()];
            let (n, _) = socket
                .try_recv_from(&mut received)
                .unwrap()
                .expect("readiness delivered data");
            assert_eq!(received.get(..n), Some(PAYLOAD));
            UDP_DELIVERED.store(true, Ordering::Release);
        })
        .unwrap();
    assert!(matches!(
        receipt.wait_blocking(),
        Ok(Admission::Admitted(_))
    ));
    let capture = Returned(returned, "busy cleanup");
    let receipt = rt
        .spawn_service_on_with_receipt(shard, async move {
            let _capture = capture;
            let mut cancel = pin!(futures::cancelled());
            let mut sent = false;
            poll_fn(|cx| match cancel.as_mut().poll(cx) {
                Poll::Pending => {
                    if !sent {
                        pending.try_send("busy").unwrap();
                        sent = true;
                    }
                    Poll::Pending
                }
                Poll::Ready(value) => Poll::Ready(value),
            })
            .await
            .unwrap();
            UDP_BUSY_STARTED.store(true, Ordering::Release);
            while !UDP_DELIVERED.load(Ordering::Acquire) {
                futures::yield_now().await;
            }
        })
        .unwrap();
    assert!(matches!(
        receipt.wait_blocking(),
        Ok(Admission::Admitted(_))
    ));
    let mut facts = [first.recv().unwrap(), first.recv().unwrap()];
    facts.sort();
    assert_eq!(facts, ["busy", "reader"]);
    thread::scope(|scope| {
        let deliver = scope.spawn(move || {
            io_first.recv().unwrap();
            while !UDP_BUSY_STARTED.load(Ordering::Acquire) {
                thread::yield_now();
            }
            assert_eq!(peer.send_to(PAYLOAD, address).unwrap(), PAYLOAD.len());
            let mut peeked = [0; PAYLOAD.len()];
            loop {
                match observed.peek_from(&mut peeked) {
                    Ok((n, _)) => {
                        assert_eq!(peeked.get(..n), Some(PAYLOAD));
                        break;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::yield_now()
                    }
                    // Consumption can precede the controller peek, but must be witnessed by
                    // the service's exact payload result; it never stops the busy sibling early.
                    Err(error) => panic!("same receive-queue observation: {error}"),
                }
                if UDP_DELIVERED.load(Ordering::Acquire) {
                    break;
                }
            }
        });
        assert!(rt.shutdown().is_ok());
        deliver.join().unwrap();
    });
    assert!(UDP_DELIVERED.load(Ordering::Acquire));
    let mut roles = [heard.try_recv().unwrap(), heard.try_recv().unwrap()];
    roles.sort();
    assert_eq!(roles, ["busy cleanup", "readiness cleanup"]);
    assert!(heard.try_recv().is_err());
}
