//! Public original-resource retirement behaviors. SOURCE ONLY: Root compiles/runs
//! with its external bounded failure supervisor. These channel fences exercise the
//! producer contract; the consumer separately uses actual Attachment Watch/I/O facts.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::disallowed_types
)]
use hyper_rt::runtime::{OriginalFence, OriginalLease, interests_for};
use hyper_rt::{RtError, Runtime, RuntimeConfig};
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::task::{Context, Poll, Waker};
use std::thread::{self, ThreadId};

fn runtime() -> Runtime {
    // Two configured public roles: a receipt owner and independently runnable sibling.
    // Native timing/page shape is inherited from existing retirement_native tests.
    Runtime::start(&RuntimeConfig {
        shards: 1,
        tasks_per_shard: 2,
        timers_per_shard: 0,
        interests_per_shard: interests_for(2),
        ring_entries: 2,
        batch: 2,
        step_budget_ns: 1_000_000,
        timer_tick_ns: 100_000,
        page_bytes: 4096,
        spin_ns: 0,
        pin: false,
        cores: Vec::new(),
        wake_tracking: None,
    })
    .unwrap()
}
struct File {
    drops: Arc<AtomicUsize>,
    closed: SyncSender<ThreadId>,
    panic: bool,
}
impl Drop for File {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        self.closed.try_send(thread::current().id()).unwrap();
        if self.panic {
            panic!("actual original Drop failure");
        }
    }
}
#[derive(Debug)]
struct FenceResult {
    terminal: bool,
    result: Result<(), RtError>,
}
#[derive(Debug)]
struct Fence {
    ready: Receiver<FenceResult>,
    entered: SyncSender<usize>,
    calls: Arc<AtomicUsize>,
    terminal: bool,
    result: Option<Result<(), RtError>>,
}
impl OriginalFence for Fence {
    fn wait_blocking(&mut self) -> Result<(), RtError> {
        // Match the actual Watch contract: an entered context refuses before any
        // terminal receipt, retained state or call/observation counter is consumed.
        if hyper_rt::registry::with_current(|_| ()).is_some() {
            return Err(RtError::NotOnShardThread);
        }
        if let Some(result) = &self.result {
            return result.clone();
        }
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.entered
            .try_send(call)
            .map_err(|_| RtError::BadConfig {
                what: "test fence observation unavailable",
            })?;
        let reply = self.ready.recv().unwrap_or(FenceResult {
            terminal: true,
            result: Err(RtError::BadConfig {
                what: "test gate closed",
            }),
        });
        self.terminal = reply.terminal;
        if self.terminal {
            self.result = Some(reply.result.clone());
        }
        reply.result
    }
    fn is_retired(&self) -> bool {
        self.terminal
    }
}
struct WorkerRelease(Option<SyncSender<()>>);
impl WorkerRelease {
    fn release(&mut self) {
        self.0.as_ref().unwrap().try_send(()).unwrap();
    }
}
impl Drop for WorkerRelease {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.try_send(());
        }
    }
}
struct OpenOnDrop(Option<SyncSender<FenceResult>>);
impl OpenOnDrop {
    fn release(&mut self, reply: FenceResult) {
        self.0.as_ref().unwrap().try_send(reply).unwrap();
    }
}
impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.try_send(FenceResult {
                terminal: true,
                result: Err(RtError::BadConfig {
                    what: "failure cleanup",
                }),
            });
        }
    }
}
struct Owners {
    file: Option<File>,
    fence: Fence,
    open: OpenOnDrop,
    entered: Receiver<usize>,
    closed: Receiver<ThreadId>,
    drops: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}

fn owners(panic: bool) -> Owners {
    let drops = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let (close, closed) = sync_channel(1);
    // Two receipts derive from the refusal-then-terminal case, not a timing retry bound.
    let (enter, entered) = sync_channel(2);
    let (release, ready) = sync_channel(1);
    Owners {
        file: Some(File {
            drops: drops.clone(),
            closed: close,
            panic,
        }),
        fence: Fence {
            ready,
            entered: enter,
            calls: calls.clone(),
            terminal: false,
            result: None,
        },
        open: OpenOnDrop(Some(release)),
        entered,
        closed,
        drops,
        calls,
    }
}

// Returning drops only this borrowed receive future, retaining the same native lease.
async fn borrow_first_pending(original: &mut OriginalLease) -> bool {
    let mut wait = pin!(original.wait());
    poll_fn(|cx| Poll::Ready(matches!(wait.as_mut().poll(cx), Poll::Pending))).await
}

fn assert_foreign_borrow_refuses(original: &mut OriginalLease) {
    let mut wait = pin!(original.wait());
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(matches!(
        wait.as_mut().poll(&mut cx),
        Poll::Ready(Err(RtError::NotOnShardThread))
    ));
}

fn complete_sibling(rt: &Runtime, shard: hyper_rt::ShardId) {
    let (command, mut receive) = hyper_rt::sync::channel(1).unwrap();
    let (progress, progressed) = sync_channel(1);
    rt.spawn_on(shard, async move {
        assert_eq!(receive.recv().await.unwrap(), 42u8);
        progress.try_send(()).unwrap();
    })
    .unwrap();
    command.try_send(42).unwrap();
    progressed.recv().unwrap();
}

fn assert_native_retirement(
    original: &mut OriginalLease,
    closed: &Receiver<ThreadId>,
    drops: &AtomicUsize,
) {
    let native = closed.recv().unwrap();
    assert_ne!(native, thread::current().id());
    original.wait_blocking().unwrap();
    assert!(original.is_retired());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn unused_adoptions_do_not_hold_shutdown_and_late_refusal_returns_whole_file_and_fence() {
    let mut rt = runtime();
    let (mut worker, mut adoption) = rt
        .prepare_retirement_with_original::<File, Fence>(&[1])
        .unwrap()
        .pop()
        .unwrap();
    rt.shutdown().unwrap();
    let Owners {
        mut file,
        fence,
        open: _open,
        entered: _entered,
        closed,
        drops,
        calls,
    } = owners(false);
    let pointer = file.as_ref().map(|file| file as *const File);
    let (fence, error) = adoption.adopt(&mut file, fence).unwrap_err();
    assert!(matches!(error, RtError::BadConfig { .. }));
    assert_eq!(file.as_ref().map(|file| file as *const File), pointer);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut handles = vec![thread::spawn(|| {})];
    assert!(worker.adopt(&mut handles).is_err());
    handles.pop().unwrap().join().unwrap();
    drop(file);
    drop(fence);
    assert_eq!(closed.recv().unwrap(), thread::current().id());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn cancelled_and_foreign_receipt_waits_keep_original_owner_until_native_fence() {
    let mut rt = runtime();
    let (mut workers, mut adoption) = rt
        .prepare_retirement_with_original::<File, Fence>(&[1])
        .unwrap()
        .pop()
        .unwrap();
    let mut handles = vec![thread::spawn(|| {})];
    workers.adopt(&mut handles).unwrap();
    let Owners {
        mut file,
        fence,
        mut open,
        entered,
        closed,
        drops,
        calls: _,
    } = owners(false);
    // This guard drops before the Runtime on failure, releasing the native reaper.
    let mut original = adoption.adopt(&mut file, fence).unwrap();
    assert!(file.is_none());
    let (back, returned) = sync_channel(1);
    let shard = rt.shard_ids()[0];
    rt.spawn_on(shard, async move {
        let pending = borrow_first_pending(&mut original).await;
        back.try_send((original, pending)).unwrap();
    })
    .unwrap();
    let (mut original, pending) = returned.recv().unwrap();
    assert!(pending);
    assert_eq!(entered.recv().unwrap(), 1);
    workers.wait_blocking().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(!original.is_retired());
    assert_foreign_borrow_refuses(&mut original);
    assert!(!original.is_retired());
    complete_sibling(&rt, shard); // Progress completes while the original fence is held.
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    open.release(FenceResult {
        terminal: true,
        result: Ok(()),
    });
    assert_native_retirement(&mut original, &closed, &drops);
    rt.shutdown().unwrap();
}

#[test]
fn raw_original_signal_joins_workers_before_owner_fence_and_preserves_first_physical_error() {
    let mut rt = runtime();
    let (mut workers, mut adoption) = rt
        .prepare_retirement_with_original::<File, Fence>(&[1])
        .unwrap()
        .pop()
        .unwrap();
    let (release_worker, held_worker) = sync_channel(1);
    let (worker_done, worker_ended) = sync_channel(1);
    let mut handles = vec![thread::spawn(move || {
        held_worker.recv().unwrap();
        worker_done.try_send(()).unwrap();
        panic!("actual worker join lifecycle failure");
    })];
    let mut release_worker = WorkerRelease(Some(release_worker));
    workers.adopt(&mut handles).unwrap();
    let Owners {
        mut file,
        fence,
        mut open,
        entered,
        closed: _closed,
        drops,
        calls: _,
    } = owners(true);
    let mut original = adoption.adopt(&mut file, fence).unwrap();
    original.request().unwrap(); // deliberately precedes worker retirement request
    assert!(entered.try_recv().is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    release_worker.release();
    worker_ended.recv().unwrap();
    assert_eq!(entered.recv().unwrap(), 1);
    assert!(workers.wait_blocking().is_err()); // separate lifecycle receipt while F is held
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let first = RtError::DriverRefused {
        call: "actual physical fence error",
        code: Some(5),
    };
    open.release(FenceResult {
        terminal: true,
        result: Err(first.clone()),
    });
    assert_eq!(original.wait_blocking(), Err(first));
    assert!(original.is_retired());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(rt.shutdown().is_err());
}

#[test]
fn nonterminal_foreign_fence_refusal_retains_file_until_explicit_retry() {
    let mut rt = runtime();
    let (mut workers, mut adoption) = rt
        .prepare_retirement_with_original::<File, Fence>(&[1])
        .unwrap()
        .pop()
        .unwrap();
    let mut handles = vec![thread::spawn(|| {})];
    workers.adopt(&mut handles).unwrap();
    let Owners {
        mut file,
        fence,
        mut open,
        entered,
        closed: _closed,
        drops,
        calls,
    } = owners(false);
    let mut original = adoption.adopt(&mut file, fence).unwrap();
    let refused = RtError::NotOnShardThread; // deliberately malformed foreign fence
    open.release(FenceResult {
        terminal: false,
        result: Err(refused.clone()),
    });
    assert_eq!(original.wait_blocking(), Err(refused));
    assert_eq!(entered.recv().unwrap(), 1);
    assert!(!original.is_retired());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    workers.wait_blocking().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1); // worker request cannot retry a refused fence
    open.release(FenceResult {
        terminal: true,
        result: Ok(()),
    });
    original.wait_blocking().unwrap();
    assert_eq!(entered.recv().unwrap(), 2);
    assert!(original.is_retired());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    rt.shutdown().unwrap();
}
