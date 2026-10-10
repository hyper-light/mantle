//! Exact hash-stream behavior through held native issuer reads, cancellation and failures.

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::branch::filter::Keys;
    use crate::store::Config;
    use hyper_block::DiskError;
    use hyper_block::buf::Alignment;
    use hyper_block::file::{CachingRequest, DeviceFile};
    use hyper_block::issuer::Issuer;
    use hyper_rt::combine::{Either, race2};
    use hyper_rt::runtime::{LocalRuntime, RuntimeConfig};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    // Config::check permits any buffered page >=4 KiB aligned to this file. The extra byte
    // makes the payload cross a format u32, independently of metadata prefix alignment.
    const CONFIG: Config = Config {
        page_size: 4096 + 1,
        extent_pages: 4,
        max_extents: 4096,
    };
    // The existing public branch_test hash-stream oracle uses this finite dataset.
    const ROWS: usize = 20_000;

    #[derive(Debug)]
    enum Notice {
        Entered,
        Deadline,
    }

    struct Gate {
        open: Mutex<bool>,
        changed: Condvar,
        armed: AtomicBool,
        // Let the first metadata page complete; hold the next required page.
        skip: AtomicUsize,
        failed: AtomicBool,
        notice: hyper_rt::sync::Sender<Notice>,
    }
    impl Gate {
        fn release(&self) {
            *self.open.lock().unwrap_or_else(|e| e.into_inner()) = true;
            self.changed.notify_all();
        }
    }
    struct OpenOnDrop(&'static Gate);
    impl Drop for OpenOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    struct Deadline {
        expired: Arc<AtomicBool>,
        stopped: Arc<(Mutex<bool>, Condvar)>,
        thread: Option<std::thread::JoinHandle<()>>,
    }
    impl Deadline {
        fn new(gate: &'static Gate) -> Self {
            let stopped = Arc::new((Mutex::new(false), Condvar::new()));
            let stop = Arc::clone(&stopped);
            let expired = Arc::new(AtomicBool::new(false));
            let timed_out = Arc::clone(&expired);
            let thread = std::thread::spawn(move || {
                let (lock, changed) = &*stop;
                let done = lock.lock().unwrap_or_else(|e| e.into_inner());
                // A failure deadline, never a gate-release ordering or success condition.
                let (done, result) = changed
                    .wait_timeout_while(done, Duration::from_secs(5), |done| !*done)
                    .unwrap_or_else(|e| e.into_inner());
                if result.timed_out() && !*done {
                    timed_out.store(true, Ordering::SeqCst);
                    let _ = gate.notice.try_send(Notice::Deadline);
                    gate.release();
                }
            });
            Self {
                expired,
                stopped,
                thread: Some(thread),
            }
        }
    }
    impl Drop for Deadline {
        fn drop(&mut self) {
            let (lock, changed) = &*self.stopped;
            *lock.lock().unwrap_or_else(|e| e.into_inner()) = true;
            changed.notify_all();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    struct Gated {
        file: DeviceFile,
        gate: &'static Gate,
    }
    struct Owner {
        store: Option<Store<Gated>>,
        gate: &'static Gate,
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.gate.release();
        }
    }
    fn refusal(op: &'static str) -> DiskError {
        DiskError::Io {
            op,
            path: std::path::PathBuf::new(),
            source: std::io::Error::other(op),
        }
    }
    impl BlockFile for Gated {
        fn alignment(&self) -> Alignment {
            self.file.alignment()
        }
        fn len(&self) -> Result<u64, DiskError> {
            self.file.len()
        }
        fn read_exact_at(&self, out: &mut [u8], at: u64) -> Result<(), DiskError> {
            if hyper_rt::futures::current_task().is_some() {
                return Err(refusal("a hash read on the runtime owner"));
            }
            if self.gate.armed.load(Ordering::SeqCst)
                && self
                    .gate
                    .skip
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                        left.checked_sub(1)
                    })
                    .is_err()
                && self.gate.armed.swap(false, Ordering::SeqCst)
            {
                let _ = self.gate.notice.try_send(Notice::Entered);
                let mut open = self.gate.open.lock().unwrap();
                while !*open {
                    open = self.gate.changed.wait(open).unwrap();
                }
                if self.gate.failed.swap(false, Ordering::SeqCst) {
                    return Err(refusal("the required hash page failed"));
                }
            }
            self.file.read_exact_at(out, at)
        }
        fn write_all_at(&self, out: &[u8], at: u64) -> Result<(), DiskError> {
            if hyper_rt::futures::current_task().is_some() {
                return Err(refusal("a hash write on the runtime owner"));
            }
            self.file.write_all_at(out, at)
        }
        fn sync_data(&self) -> Result<(), DiskError> {
            if hyper_rt::futures::current_task().is_some() {
                return Err(refusal("a hash flush on the runtime owner"));
            }
            self.file.sync_data()
        }
        fn try_clone(&self) -> Result<Self, DiskError> {
            Ok(Self {
                file: self.file.try_clone()?,
                gate: self.gate,
            })
        }
    }
    fn key(i: usize) -> Vec<u8> {
        let mut key = vec![0, 255];
        key.extend_from_slice(&u32::try_from(i).unwrap().to_be_bytes());
        key.extend_from_slice(&[0, 255]);
        key
    }
    fn expected(i: usize) -> Vec<u8> {
        vec![u8::try_from(i % 251).unwrap(); 100]
    }
    async fn collect(
        store: &mut Store<Gated>,
        branch: &Branch,
        cursor: &mut HashCursor,
        out: &mut Vec<u32>,
    ) -> Result<(), Error> {
        loop {
            match cursor.next_paced(store, branch)? {
                HashStep::Done(Some(hash)) => out.push(hash),
                HashStep::Done(None) => return Ok(()),
                HashStep::Waiting => {
                    if !store.wait_completion().await? {
                        return Err(Error::InvalidArgument {
                            what: "a hash page without a completion owner",
                        });
                    }
                }
            }
        }
    }
    fn held_case(fail: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hash-pages");
        let (notice, mut heard) = hyper_rt::sync::channel(1).unwrap();
        let gate = Box::leak(Box::new(Gate {
            open: Mutex::new(true),
            changed: Condvar::new(),
            armed: AtomicBool::new(false),
            skip: AtomicUsize::new(1),
            failed: AtomicBool::new(fail),
            notice,
        }));
        let gate: &'static Gate = gate;
        let file = Gated {
            file: DeviceFile::open(
                &path,
                true,
                CachingRequest::Buffered,
                Alignment::new(1).unwrap(),
            )
            .unwrap(),
            gate,
        };
        let mut store = Store::create(file, CONFIG).unwrap();
        store.set_cache(0);
        let mut builder =
            Builder::new(&mut store, Keys::Exactly(u64::try_from(ROWS).unwrap())).unwrap();
        let mut want = Vec::with_capacity(ROWS);
        for i in 0..ROWS {
            let key = key(i);
            want.push(crate::maplet::hash32(filter::hash(&key)));
            let op = if i % 7 == 0 { Op::Delete } else { Op::Put };
            builder
                .add(
                    &mut store,
                    &key,
                    op,
                    &if op == Op::Put {
                        expected(i)
                    } else {
                        Vec::new()
                    },
                )
                .unwrap();
        }
        want.sort_unstable();
        let branch = builder.finish(&mut store).unwrap();
        let mut descriptor = Vec::new();
        branch.encode(&mut descriptor).unwrap();
        store.checkpoint(Some(branch.root), 7).unwrap();
        let issuer = Issuer::start_for(dir.path(), 1, 1).unwrap();
        store.attach(&issuer, 1).unwrap();
        let config = RuntimeConfig {
            shards: 1,
            tasks_per_shard: 2,
            timers_per_shard: 2,
            interests_per_shard: 4,
            ring_entries: 2,
            batch: 2,
            step_budget_ns: 1_000_000_000,
            timer_tick_ns: 100_000,
            spin_ns: 0,
            page_bytes: 4096,
            wake_tracking: None,
            pin: false,
            cores: Vec::new(),
        };
        let mut runtime = LocalRuntime::new(&config).unwrap();
        let _open = OpenOnDrop(gate);
        *gate.open.lock().unwrap() = false;
        gate.armed.store(true, Ordering::SeqCst);
        let deadline = Deadline::new(gate);
        let mut owner = Owner {
            store: Some(store),
            gate,
        };
        let (store, branch, want) = runtime
            .block_on(async move {
                let store = owner.store.as_mut().unwrap();
                let mut cursor = branch.hashes(store).unwrap();
                let mut got = Vec::new();
                {
                    let read = collect(store, &branch, &mut cursor, &mut got);
                    let mut read = std::pin::pin!(read);
                    match race2(read.as_mut(), heard.recv()).await {
                        Either::Second(Ok(Notice::Entered)) => {}
                        other => panic!("required callback was not held: {other:?}"),
                    }
                    // Cancellation drops the borrowed future, preserving the same cursor/receipt.
                }
                assert_eq!(got, want[..got.len()]);
                let (sent, mut progressed) = hyper_rt::sync::channel(1).unwrap();
                hyper_rt::futures::spawn_detached(async move {
                    sent.try_send(()).unwrap();
                    gate.release();
                })
                .unwrap();
                let result = collect(store, &branch, &mut cursor, &mut got).await;
                progressed.recv().await.unwrap();
                cursor.give_back(store);
                if fail {
                    assert!(matches!(result, Err(Error::Io { .. })));
                    assert_eq!(got, want[..got.len()]);
                    let mut repaired = branch.hashes(store).unwrap();
                    got.clear();
                    collect(store, &branch, &mut repaired, &mut got)
                        .await
                        .unwrap();
                    repaired.give_back(store);
                } else {
                    result.unwrap();
                }
                assert_eq!(got, want);
                assert_eq!(store.io_stats().buffers_out, 0);
                (owner.store.take().unwrap(), branch, want)
            })
            .unwrap();
        let expired = Arc::clone(&deadline.expired);
        drop(deadline);
        assert!(
            !expired.load(Ordering::SeqCst),
            "the failure deadline opened the callback"
        );
        let (file, landed) = crate::store::cold_file(store.into_file());
        landed.unwrap();
        drop(file);
        let file = Gated {
            file: DeviceFile::open(
                &path,
                false,
                CachingRequest::Buffered,
                Alignment::new(1).unwrap(),
            )
            .unwrap(),
            gate,
        };
        let (mut reopened, checkpoint) = Store::open(file, CONFIG).unwrap();
        assert_eq!(checkpoint.applied, 7);
        let (decoded, used) = Branch::decode(&mut reopened, &descriptor).unwrap();
        assert_eq!(used, descriptor.len());
        assert_eq!(decoded, branch);
        let mut cursor = decoded.hashes(&reopened).unwrap();
        let mut got = Vec::new();
        while let Some(hash) = cursor.next(&mut reopened, &decoded).unwrap() {
            got.push(hash);
        }
        assert_eq!(got, want);
        let mut value = Vec::new();
        for i in 0..ROWS {
            value.clear();
            let op = decoded.get(&mut reopened, &key(i), &mut value).unwrap();
            if i % 7 == 0 {
                assert_eq!(op, Some(Op::Delete));
                assert!(value.is_empty());
            } else {
                assert_eq!(op, Some(Op::Put));
                assert_eq!(value, expected(i));
            }
        }
    }
    #[test]
    fn held_hash_page_preserves_cross_page_values_through_cancel_and_reopen() {
        held_case(false);
    }
    #[test]
    fn required_hash_page_error_returns_owners_then_healthy_stream_reopens_exactly() {
        held_case(true);
    }
}
